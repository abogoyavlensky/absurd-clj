(ns absurd.task-result-test
  (:require [absurd.core :as absurd]
            [absurd.sql :as sql]
            [absurd.test-helpers :as h]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(use-fixtures :each h/with-conn)

(def ^:private make-context (ns-resolve 'absurd.core 'make-context))

(deftest fetch-task-result-tracks-lifecycle
  (let [app (-> (h/test-client) (absurd/register-task! "result-target" (fn [_ _] {:ok true})))
        spawned (absurd/spawn! app "result-target" {})]
    (is (= {:state :pending} (absurd/fetch-task-result app (:task-id spawned))))
    (let [[claim] (absurd/claim-tasks! app {:worker-id "worker"})]
      (is (= {:state :running} (absurd/fetch-task-result app (:task-id spawned))))
      (sql/complete-run! h/*conn* (:queue app) (:run-id claim) {:ok true}))
    (is (= {:state :completed
            :result {:ok true}} (absurd/fetch-task-result app (:task-id spawned))))))

(deftest fetch-task-result-reports-failures
  (let [app (-> (h/test-client)
                (absurd/register-task! "boom" (fn [_ _] (throw (RuntimeException. "boom"))) {:max-attempts 1}))
        spawned (absurd/spawn! app "boom" {})]
    (absurd/work-batch! app {:worker-id "worker"})
    (let [{:keys [state failure]} (absurd/fetch-task-result app (:task-id spawned))]
      (is (= :failed state))
      (is (= "boom" (:message failure))))))

(deftest task-context-await-task-result-waits-for-terminal-state
  (let [parent (h/test-client {:queue (h/random-queue "parent")})
        child (h/test-client {:queue (h/random-queue "child")})
        _ (absurd/spawn! parent "parent" {} {:queue (:queue parent)})
        [parent-claim] (absurd/claim-tasks! parent {:worker-id "parent-worker"})
        ctx (make-context parent parent-claim 120 nil)
        child-spawned (absurd/spawn! child "child" {} {:queue (:queue child)})
        finish-child (future
                       (with-open [conn (jdbc/get-connection (h/datasource))]
                         (Thread/sleep 100)
                         (let [[claim] (sql/claim-tasks! conn (:queue child) "child-worker" 120 1)]
                           (sql/complete-run! conn (:queue child) (:run-id claim) {:child "ok"}))))]
    (try
      (is (= {:state :completed
              :result {:child "ok"}}
             (absurd/await-task-result ctx (:task-id child-spawned) {:queue (:queue child)
                                                                     :timeout 2})))
      (finally
        @finish-child))
    (testing "the checkpoint stores the state as a string, byte-compatible with Python"
      (is (= [{:checkpoint-name (str "$awaitTaskResult:" (:task-id child-spawned))
               :state {:state "completed"
                       :result {:child "ok"}}}]
             (mapv #(select-keys % [:checkpoint-name :state])
                   (h/get-checkpoints h/*conn* (:queue parent) (:task-id parent-claim))))))
    (testing "replay returns the keyword state from the checkpoint"
      (let [resumed (make-context parent parent-claim 120 nil)]
        (is (= {:state :completed
                :result {:child "ok"}}
               (absurd/await-task-result resumed (:task-id child-spawned) {:queue (:queue child)
                                                                           :timeout 2})))))))

(deftest task-context-await-task-result-rejects-same-queue
  (let [app (h/test-client)
        _ (absurd/spawn! app "parent" {} {:queue (:queue app)})
        [claim] (absurd/claim-tasks! app {:worker-id "parent-worker"})
        ctx (make-context app claim 120 nil)
        child (absurd/spawn! app "child" {} {:queue (:queue app)})]
    (is (= :absurd/same-queue-wait
           (h/ex-type #(absurd/await-task-result ctx (:task-id child) {:timeout 30}))))
    (is (= :absurd/same-queue-wait
           (h/ex-type #(absurd/await-task-result ctx (:task-id child) {:queue (:queue app)
                                                                       :timeout 30}))))))

(deftest task-context-await-task-result-checkpoint-survives-child-cleanup-on-retry
  (let [parent (h/test-client {:queue (h/random-queue "parent")})
        child (-> (h/test-client {:queue (h/random-queue "child")})
                  (absurd/register-task! "child" (fn [_ _] {:child "ok"})))
        _ (absurd/register-task! parent "parent"
                                 (fn [params ctx]
                                   (let [snapshot (absurd/await-task-result ctx (:child_id params)
                                                                            {:queue (:queue child)
                                                                             :timeout 5})]
                                     (when (= 1 (:attempt ctx))
                                       (throw (RuntimeException. "retry after child result observed")))
                                     (:result snapshot)))
                                 {:max-attempts 2})
        child-spawned (absurd/spawn! child "child" nil)
        _ (absurd/work-batch! child {:worker-id "child-worker"})
        parent-spawned (absurd/spawn! parent "parent" {:child_id (:task-id child-spawned)})]
    (absurd/work-batch! parent {:worker-id "parent-worker"})
    (is (= 1 (h/count-checkpoints h/*conn* (:queue parent) (:task-id parent-spawned))))
    (is (= 1 (:n (jdbc/execute-one! h/*conn* ["select absurd.cleanup_tasks(?, ?::integer, ?::integer) as n" (:queue child) 0 10]
                                    {:builder-fn rs/as-unqualified-kebab-maps}))))
    (is (nil? (absurd/fetch-task-result child (:task-id child-spawned))))
    (absurd/work-batch! parent {:worker-id "parent-worker"})
    (is (= {:state :completed
            :result {:child "ok"}}
           (absurd/await-task-result parent (:task-id parent-spawned) {:timeout 5})))))

(deftest task-context-await-task-result-heartbeats-while-waiting
  (let [parent (h/test-client {:queue (h/random-queue "parent")})
        child-queue (h/random-queue "child")
        _ (absurd/create-queue! parent child-queue)
        _ (absurd/register-task! parent "parent"
                                 (fn [params ctx]
                                   (:result (absurd/await-task-result ctx (:child_id params)
                                                                      {:queue child-queue
                                                                       :timeout 10}))))
        child-spawned (absurd/spawn! parent "slow-child" nil {:queue child-queue})
        parent-spawned (absurd/spawn! parent "parent" {:child_id (:task-id child-spawned)})
        slow-child (future
                     (with-open [conn (jdbc/get-connection (h/datasource))]
                       (let [[claim] (sql/claim-tasks! conn child-queue "child-worker" 120 1)]
                         (Thread/sleep 3000)
                         (sql/complete-run! conn child-queue (:run-id claim) {:child "ok"}))))]
    (try
      (absurd/work-batch! parent {:worker-id "parent-worker"
                                  :claim-timeout 1})
      (finally
        @slow-child))
    (testing "the parent run stayed alive past its 1 s claim timeout thanks to heartbeats"
      (is (= {:state :completed
              :result {:child "ok"}}
             (absurd/await-task-result parent (:task-id parent-spawned) {:timeout 5})))
      (is (= 1 (count (h/get-runs h/*conn* (:queue parent) (:task-id parent-spawned))))))))

(deftest task-context-await-task-result-keeps-lease-alive-under-competing-claims
  (let [parent (h/test-client {:queue (h/random-queue "parent")})
        child-queue (h/random-queue "child")
        _ (absurd/create-queue! parent child-queue)
        _ (absurd/register-task! parent "parent"
                                 (fn [params ctx]
                                   ;; Use up most of the 2 s claim before waiting.
                                   (Thread/sleep 1500)
                                   (:result (absurd/await-task-result ctx (:child_id params)
                                                                      {:queue child-queue
                                                                       :timeout 10})))
                                 {:max-attempts 1})
        child-spawned (absurd/spawn! parent "slow-child" nil {:queue child-queue})
        parent-spawned (absurd/spawn! parent "parent" {:child_id (:task-id child-spawned)})
        stop? (atom false)
        competitor (future
                     (with-open [conn (jdbc/get-connection (h/datasource))]
                       (Thread/sleep 300)
                       (while (not @stop?)
                         (sql/claim-tasks! conn (:queue parent) "competitor" 2 1)
                         (Thread/sleep 100))))
        child-worker (future
                       (with-open [conn (jdbc/get-connection (h/datasource))]
                         (let [[claim] (sql/claim-tasks! conn child-queue "child-worker" 120 1)]
                           (Thread/sleep 2500)
                           (sql/complete-run! conn child-queue (:run-id claim) {:child "ok"}))))]
    (try
      (absurd/work-batch! parent {:worker-id "parent-worker"
                                  :claim-timeout 2})
      (finally
        (reset! stop? true)
        @competitor
        @child-worker))
    (is (= {:state :completed
            :result {:child "ok"}}
           (absurd/await-task-result parent (:task-id parent-spawned) {:timeout 5})))
    (is (= 1 (count (h/get-runs h/*conn* (:queue parent) (:task-id parent-spawned)))))))
