(ns absurd.heartbeat-test
  (:require [absurd.core :as absurd]
            [absurd.sql :as sql]
            [absurd.test-helpers :as h]
            [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]])
  (:import [java.time Instant]))

(use-fixtures :each h/with-conn)

(def ^:private make-context (ns-resolve 'absurd.core 'make-context))
(def ^:private execute-task! (ns-resolve 'absurd.core 'execute-task!))

(defn- fail-run-directly! [app run-id]
  (sql/fail-run! h/*conn* (:queue app) run-id {:name "$ClaimTimeout"
                                               :message "timeout"}))

(deftest heartbeat-zero-is-rejected-by-sql
  (let [app (-> (h/test-client)
                (absurd/register-task! "bad-heartbeat" (fn [_ ctx] (absurd/heartbeat! ctx 0) {:ok true})
                                       {:max-attempts 1}))
        spawned (absurd/spawn! app "bad-heartbeat" {})]
    (absurd/work-batch! app {:worker-id "worker"
                             :claim-timeout 60})
    (is (= "failed" (:state (h/get-task h/*conn* (:queue app) (:task-id spawned)))))
    (let [run (h/get-run h/*conn* (:queue app) (:run-id spawned))]
      (is (= "failed" (:state run)))
      (is (str/includes? (get-in run [:failure-reason :message]) "extend_by must be > 0")))))

(deftest heartbeat-on-failed-run-raises-failed-run
  (let [app (-> (h/test-client) (absurd/register-task! "heartbeat-task" (fn [_ _] {:ok true})))
        spawned (absurd/spawn! app "heartbeat-task" {:value 1})
        [claim] (absurd/claim-tasks! app {:worker-id "worker"
                                          :claim-timeout 60})]
    (fail-run-directly! app (:run-id claim))
    (let [ctx (make-context app claim 60 nil)]
      (is (= :absurd/failed-run (h/ex-type #(absurd/heartbeat! ctx 30)))))
    (is (= "failed" (:state (h/get-run h/*conn* (:queue app) (:run-id spawned)))))))

(deftest heartbeat-defaults-to-the-claim-timeout
  (let [app (h/test-client)
        _ (absurd/spawn! app "lease" {} {:queue (:queue app)})
        [claim] (absurd/claim-tasks! app {:worker-id "worker"
                                          :claim-timeout 10})
        ctx (make-context app claim 600 nil)
        before (Instant/now)]
    (absurd/heartbeat! ctx)
    (let [expires (.toInstant ^java.sql.Timestamp (:claim-expires-at (h/get-run h/*conn* (:queue app) (:run-id claim))))]
      (is (.isAfter expires (.plusSeconds before 590))))))

(deftest execute-task-swallows-failed-run-on-checkpoint
  (let [app (-> (h/test-client)
                (absurd/register-task! "checkpoint-failed"
                                       (fn [_ ctx] (absurd/step ctx "persist" (fn [] {:value 1})) {:ok true})))
        spawned (absurd/spawn! app "checkpoint-failed" {:value 1})
        [claim] (absurd/claim-tasks! app {:worker-id "worker"
                                          :claim-timeout 60})]
    (fail-run-directly! app (:run-id claim))
    (is (nil? (execute-task! app claim 60 nil)))
    (is (= 0 (h/count-checkpoints h/*conn* (:queue app) (:task-id spawned))))))

(deftest execute-task-swallows-cancelled-on-complete-run
  (let [app (-> (h/test-client) (absurd/register-task! "complete-cancel" (fn [_ _] {:ok true})))
        spawned (absurd/spawn! app "complete-cancel" {:value 1})
        [claim] (absurd/claim-tasks! app {:worker-id "worker"
                                          :claim-timeout 60})]
    (absurd/cancel-task! app (:task-id spawned))
    (is (nil? (execute-task! app claim 60 nil)))
    (is (= "cancelled" (:state (h/get-run h/*conn* (:queue app) (:run-id spawned)))))
    (is (= "cancelled" (:state (h/get-task h/*conn* (:queue app) (:task-id spawned)))))))

(deftest execute-task-swallows-cancelled-on-fail-run
  (let [app (-> (h/test-client) (absurd/register-task! "fail-cancel" (fn [_ _] (throw (RuntimeException. "boom")))))
        spawned (absurd/spawn! app "fail-cancel" {:value 1})
        [claim] (absurd/claim-tasks! app {:worker-id "worker"
                                          :claim-timeout 60})]
    (absurd/cancel-task! app (:task-id spawned))
    (is (nil? (execute-task! app claim 60 nil)))
    (is (= "cancelled" (:state (h/get-run h/*conn* (:queue app) (:run-id spawned)))))
    (is (= "cancelled" (:state (h/get-task h/*conn* (:queue app) (:task-id spawned)))))))
