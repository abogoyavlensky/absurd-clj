(ns absurd.sql-test
  (:require [absurd.sql :as sql]
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
  (let [queue (h/random-queue "sqlq")]
    (sql/create-queue! h/*conn* queue nil)
    (is (some #{queue} (sql/list-queues h/*conn*)))
    (testing "policy intervals come back as strings"
      (let [policy (sql/get-queue-policy h/*conn* queue)]
        (is (= queue (:queue-name policy)))
        (is (= "unpartitioned" (:storage-mode policy)))
        (is (string? (:cleanup-ttl policy)))
        (is (int? (:cleanup-limit policy)))))
    (sql/set-queue-policy! h/*conn* queue {"cleanup_ttl" "3 days"
                                           "cleanup_limit" 7})
    (is (= 7 (:cleanup-limit (sql/get-queue-policy h/*conn* queue))))
    (is (= "3 days" (:cleanup-ttl (sql/get-queue-policy h/*conn* queue))))
    (is (nil? (sql/get-queue-policy h/*conn* (h/random-queue "missing"))))
    (sql/drop-queue! h/*conn* queue)
    (is (not (some #{queue} (sql/list-queues h/*conn*))))))

(deftest spawn-claim-checkpoint-complete-flow
  (let [queue (h/random-queue "sqlf")]
    (sql/create-queue! h/*conn* queue nil)
    (let [spawned (sql/spawn-task! h/*conn* queue "echo" {:value 1
                                                          :tags [:a]}
                                   {"headers" {:trace-id "t1"}
                                    "max_attempts" 3})]
      (is (string? (:task-id spawned)))
      (is (string? (:run-id spawned)))
      (is (= 1 (:attempt spawned)))
      (is (true? (:created spawned)))
      (let [[claim :as claims] (sql/claim-tasks! h/*conn* queue "w1" 60 5)]
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
        (is (empty? (sql/claim-tasks! h/*conn* queue "w1" 60 5)))
        (testing "checkpoints persist and decode"
          (sql/set-task-checkpoint-state! h/*conn* queue (:task-id claim) "step-1"
                                          {:ok true} (:run-id claim) 60)
          (is (= {:checkpoint-name "step-1"
                  :state {:ok true}}
                 (-> (sql/get-task-checkpoint-state h/*conn* queue (:task-id claim) "step-1")
                     (select-keys [:checkpoint-name :state]))))
          (is (nil? (sql/get-task-checkpoint-state h/*conn* queue (:task-id claim) "nope")))
          (is (= [{:checkpoint-name "step-1"
                   :state {:ok true}}]
                 (mapv #(select-keys % [:checkpoint-name :state])
                       (sql/get-task-checkpoint-states h/*conn* queue (:task-id claim) (:run-id claim))))))
        (testing "task result before and after completion"
          (is (= "running" (:state (sql/get-task-result h/*conn* queue (:task-id claim)))))
          (sql/complete-run! h/*conn* queue (:run-id claim) {:done true})
          (let [result (sql/get-task-result h/*conn* queue (:task-id claim))]
            (is (= "completed" (:state result)))
            (is (= {:done true} (:result result)))
            (is (= (:task-id claim) (:task-id result)))))))
    (is (nil? (sql/get-task-result h/*conn* queue (str (random-uuid)))))))

(deftest terminal-run-states-map-to-signals
  (let [queue (h/random-queue "sqls")]
    (sql/create-queue! h/*conn* queue nil)
    (testing "completing an already failed run signals failed-run"
      (let [{:keys [run-id]} (sql/spawn-task! h/*conn* queue "t" nil {"max_attempts" 1})
            [claim] (sql/claim-tasks! h/*conn* queue "w" 60 1)]
        (is (= run-id (:run-id claim)))
        (sql/fail-run! h/*conn* queue run-id {:name "Boom"
                                              :message "boom"
                                              :traceback nil})
        (is (= :absurd/failed-run
               (ex-type #(sql/complete-run! h/*conn* queue run-id nil))))
        (is (= :absurd/failed-run
               (ex-type #(sql/extend-claim! h/*conn* queue run-id 30))))
        (let [result (sql/get-task-result h/*conn* queue (:task-id claim))]
          (is (= "failed" (:state result)))
          (is (= "boom" (get-in result [:failure-reason :message]))))))
    (testing "cancelled task rejects checkpoint writes"
      (let [{:keys [task-id run-id]} (sql/spawn-task! h/*conn* queue "t" nil {})]
        (sql/claim-tasks! h/*conn* queue "w" 60 1)
        (sql/cancel-task! h/*conn* queue task-id)
        (is (= :absurd/cancelled
               (ex-type #(sql/set-task-checkpoint-state! h/*conn* queue task-id "s" 1 run-id 60))))
        (is (= "cancelled" (:state (sql/get-task-result h/*conn* queue task-id))))))))

(deftest events-and-scheduling
  (let [queue (h/random-queue "sqle")]
    (sql/create-queue! h/*conn* queue nil)
    (let [{:keys [task-id run-id]} (sql/spawn-task! h/*conn* queue "t" nil {})]
      (sql/claim-tasks! h/*conn* queue "w" 60 1)
      (testing "await then emit"
        (is (= {:should-suspend true
                :payload nil}
               (sql/await-event h/*conn* queue task-id run-id "$awaitEvent:ping" "ping" nil)))
        (is (= "sleeping" (:state (sql/get-task-result h/*conn* queue task-id))))
        (sql/emit-event! h/*conn* queue "ping" {:n 1})
        (testing "emit clears wake_event, stores the payload and the checkpoint"
          (let [[claim] (sql/claim-tasks! h/*conn* queue "w" 60 1)]
            (is (nil? (:wake-event claim)))
            (is (= {:n 1} (:event-payload claim))))
          (is (= {:n 1} (:state (sql/get-task-checkpoint-state
                                  h/*conn* queue task-id "$awaitEvent:ping")))))
        (is (= {:should-suspend false
                :payload {:n 1}}
               (sql/await-event h/*conn* queue task-id run-id "$awaitEvent:ping" "ping" nil))))
      (testing "schedule-run-after uses the database clock"
        (let [base (Instant/parse "2030-01-01T00:00:00Z")]
          (h/set-fake-now! h/*conn* base)
          (is (= base (sql/current-time h/*conn*)))
          (sql/schedule-run-after! h/*conn* queue run-id 90)
          (let [run (h/get-run h/*conn* queue run-id)]
            (is (= "sleeping" (:state run)))
            (is (= (.plusSeconds base 90) (.toInstant ^java.sql.Timestamp (:available-at run))))))))))

(deftest retry-task-wrapper
  (let [queue (h/random-queue "sqlr")]
    (sql/create-queue! h/*conn* queue nil)
    (let [{:keys [task-id run-id]} (sql/spawn-task! h/*conn* queue "t" nil {"max_attempts" 1})]
      (sql/claim-tasks! h/*conn* queue "w" 60 1)
      (sql/fail-run! h/*conn* queue run-id {:name "E"
                                            :message "m"})
      (let [retried (sql/retry-task! h/*conn* queue task-id {})]
        (is (= task-id (:task-id retried)))
        (is (= 2 (:attempt retried)))
        (is (false? (:created retried)))
        (is (string? (:run-id retried)))))))
