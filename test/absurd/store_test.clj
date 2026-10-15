(ns absurd.store-test
  (:require [absurd.sql :as sql]
            [absurd.store :as store]
            [absurd.test-helpers :as h]
            [clojure.test :refer [deftest is testing use-fixtures]])
  (:import [java.time Instant]))

(use-fixtures :each h/with-conn)

(defn- ex-type [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (:type (ex-data e)))))

(deftest with-task-state-errors-works-from-other-namespaces
  (is (= :ok (sql/with-task-state-errors :ok)))
  (is (= :absurd/cancelled
         (ex-type #(sql/with-task-state-errors
                     (throw (java.sql.SQLException. "cancelled" "AB001"))))))
  (is (thrown? java.sql.SQLException
               (sql/with-task-state-errors
                 (throw (java.sql.SQLException. "other" "42P01"))))))

(deftest queue-management-round-trip
  (let [queue (h/random-queue "sqlq")
        st (h/store)]
    (store/create-queue! st queue nil)
    (is (some #{queue} (store/list-queues st)))
    (testing "policy intervals come back as strings"
      (let [policy (store/get-queue-policy st queue)]
        (is (= queue (:queue-name policy)))
        (is (= "unpartitioned" (:storage-mode policy)))
        (is (string? (:cleanup-ttl policy)))
        (is (int? (:cleanup-limit policy)))))
    (store/set-queue-policy! st queue {"cleanup_ttl" "3 days"
                                       "cleanup_limit" 7})
    (is (= 7 (:cleanup-limit (store/get-queue-policy st queue))))
    (is (= "3 days" (:cleanup-ttl (store/get-queue-policy st queue))))
    (is (nil? (store/get-queue-policy st (h/random-queue "missing"))))
    (store/drop-queue! st queue)
    (is (not (some #{queue} (store/list-queues st))))))

(deftest spawn-claim-checkpoint-complete-flow
  (let [queue (h/random-queue "sqlf")
        st (h/store)]
    (store/create-queue! st queue nil)
    (let [spawned (store/spawn-task! st queue "echo" {:value 1
                                                      :tags [:a]}
                                     {"headers" {:trace-id "t1"}
                                      "max_attempts" 3})]
      (is (string? (:task-id spawned)))
      (is (string? (:run-id spawned)))
      (is (= 1 (:attempt spawned)))
      (is (true? (:created spawned)))
      (let [[claim :as claims] (store/claim-tasks! st queue "w1" 60 5)]
        (is (= 1 (count claims)))
        (is (= (:task-id spawned) (:task-id claim)))
        (is (= (:run-id spawned) (:run-id claim)))
        (is (= "echo" (:task-name claim)))
        (is (= {:value 1
                :tags ["a"]} (:params claim)))
        (is (= {:trace-id "t1"} (:headers claim)))
        (is (= 3 (:max-attempts claim)))
        (is (nil? (:wake-event claim)))
        (is (nil? (:event-payload claim)))
        (is (empty? (store/claim-tasks! st queue "w1" 60 5)))
        (testing "checkpoints persist and decode"
          (store/set-task-checkpoint-state! st queue (:task-id claim) "step-1"
                                            {:ok true} (:run-id claim) 60)
          (is (= {:checkpoint-name "step-1"
                  :state {:ok true}}
                 (-> (store/get-task-checkpoint-state st queue (:task-id claim) "step-1")
                     (select-keys [:checkpoint-name :state]))))
          (is (nil? (store/get-task-checkpoint-state st queue (:task-id claim) "nope")))
          (is (= [{:checkpoint-name "step-1"
                   :state {:ok true}}]
                 (mapv #(select-keys % [:checkpoint-name :state])
                       (store/get-task-checkpoint-states st queue (:task-id claim) (:run-id claim))))))
        (testing "task result before and after completion"
          (is (= "running" (:state (store/get-task-result st queue (:task-id claim)))))
          (store/complete-run! st queue (:run-id claim) {:done true})
          (let [result (store/get-task-result st queue (:task-id claim))]
            (is (= "completed" (:state result)))
            (is (= {:done true} (:result result)))
            (is (= (:task-id claim) (:task-id result)))))))
    (is (nil? (store/get-task-result st queue (str (random-uuid)))))))

(deftest terminal-run-states-map-to-signals
  (let [queue (h/random-queue "sqls")
        st (h/store)]
    (store/create-queue! st queue nil)
    (testing "completing an already failed run signals failed-run"
      (let [{:keys [run-id]} (store/spawn-task! st queue "t" nil {"max_attempts" 1})
            [claim] (store/claim-tasks! st queue "w" 60 1)]
        (is (= run-id (:run-id claim)))
        (store/fail-run! st queue run-id {:name "Boom"
                                          :message "boom"
                                          :traceback nil})
        (is (= :absurd/failed-run
               (ex-type #(store/complete-run! st queue run-id nil))))
        (is (= :absurd/failed-run
               (ex-type #(store/extend-claim! st queue run-id 30))))
        (let [result (store/get-task-result st queue (:task-id claim))]
          (is (= "failed" (:state result)))
          (is (= "boom" (get-in result [:failure-reason :message]))))))
    (testing "cancelled task rejects checkpoint writes"
      (let [{:keys [task-id run-id]} (store/spawn-task! st queue "t" nil {})]
        (store/claim-tasks! st queue "w" 60 1)
        (store/cancel-task! st queue task-id)
        (is (= :absurd/cancelled
               (ex-type #(store/set-task-checkpoint-state! st queue task-id "s" 1 run-id 60))))
        (is (= "cancelled" (:state (store/get-task-result st queue task-id))))))))

(deftest events-and-scheduling
  (let [queue (h/random-queue "sqle")
        st (h/store)]
    (store/create-queue! st queue nil)
    (let [{:keys [task-id run-id]} (store/spawn-task! st queue "t" nil {})]
      (store/claim-tasks! st queue "w" 60 1)
      (testing "await then emit"
        (is (= {:should-suspend true
                :payload nil}
               (store/await-event st queue task-id run-id "$awaitEvent:ping" "ping" nil)))
        (is (= "sleeping" (:state (store/get-task-result st queue task-id))))
        (store/emit-event! st queue "ping" {:n 1})
        (testing "emit clears wake_event, stores the payload and the checkpoint"
          (let [[claim] (store/claim-tasks! st queue "w" 60 1)]
            (is (nil? (:wake-event claim)))
            (is (= {:n 1} (:event-payload claim))))
          (is (= {:n 1} (:state (store/get-task-checkpoint-state
                                  st queue task-id "$awaitEvent:ping")))))
        (is (= {:should-suspend false
                :payload {:n 1}}
               (store/await-event st queue task-id run-id "$awaitEvent:ping" "ping" nil))))
      (testing "schedule-run-after uses the database clock"
        (let [base (Instant/parse "2030-01-01T00:00:00Z")]
          (h/set-fake-now! h/*conn* base)
          (is (= base (store/current-time st)))
          (store/schedule-run-after! st queue run-id 90)
          (let [run (h/get-run h/*conn* queue run-id)]
            (is (= "sleeping" (:state run)))
            (is (= (.plusSeconds base 90) (:available-at run)))))))))

(deftest retry-task-wrapper
  (let [queue (h/random-queue "sqlr")
        st (h/store)]
    (store/create-queue! st queue nil)
    (let [{:keys [task-id run-id]} (store/spawn-task! st queue "t" nil {"max_attempts" 1})]
      (store/claim-tasks! st queue "w" 60 1)
      (store/fail-run! st queue run-id {:name "E"
                                        :message "m"})
      (let [retried (store/retry-task! st queue task-id {})]
        (is (= task-id (:task-id retried)))
        (is (= 2 (:attempt retried)))
        (is (false? (:created retried)))
        (is (string? (:run-id retried)))))))
