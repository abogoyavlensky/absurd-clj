(ns absurd.retry-test
  (:require [absurd.core :as absurd]
            [absurd.store :as store]
            [absurd.test-helpers :as h]
            [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]])
  (:import [java.time Instant]))

(use-fixtures :each h/with-conn)

(defn- work! [app]
  (absurd/work-batch! app {:worker-id "worker1"
                           :claim-timeout 60
                           :batch-size 1}))

(defn- task [app spawned]
  (h/get-task h/*conn* (:queue app) (:task-id spawned)))

(deftest fail-run-without-strategy-requeues-immediately
  (let [attempts (atom 0)
        app (-> (h/test-client)
                (absurd/register-task! "no-strategy"
                                       (fn [_ _]
                                         (when (< (swap! attempts inc) 2)
                                           (throw (ex-info "boom" {})))
                                         {:attempts @attempts})
                                       {:max-attempts 3}))
        spawned (absurd/spawn! app "no-strategy" {:payload 1})]
    (work! app)
    (let [t (task app spawned)]
      (is (= "pending" (:state t)))
      (is (= 2 (:attempts t))))
    (work! app)
    (let [t (task app spawned)]
      (is (= "completed" (:state t)))
      (is (= 2 (:attempts t)))
      (is (= {:attempts 2} (:completed-payload t))))))

(deftest exponential-backoff-retry-strategy
  (let [base (Instant/parse "2024-05-01T10:00:00Z")
        attempts (atom 0)
        app (-> (h/test-client)
                (absurd/register-task! "exp-backoff"
                                       (fn [_ _]
                                         (when (< (swap! attempts inc) 3)
                                           (throw (ex-info (str "fail-" @attempts) {})))
                                         {:success true})))
        _ (h/set-fake-now! h/*conn* base)
        spawned (absurd/spawn! app "exp-backoff" nil
                               {:max-attempts 3
                                :retry-strategy {:kind :exponential
                                                 :base-seconds 40
                                                 :factor 2}})]
    (work! app)
    (let [t (task app spawned)]
      (is (= "sleeping" (:state t)))
      (is (= 2 (:attempts t))))
    (h/set-fake-now! h/*conn* (.plusSeconds base 40))
    (work! app)
    (let [t (task app spawned)]
      (is (= "sleeping" (:state t)))
      (is (= 3 (:attempts t))))
    (h/set-fake-now! h/*conn* (.plusSeconds base 120))
    (work! app)
    (let [t (task app spawned)]
      (is (= "completed" (:state t)))
      (is (= 3 (:attempts t)))
      (is (= {:success true} (:completed-payload t))))))

(deftest fixed-backoff-retry-strategy
  (let [base (Instant/parse "2024-05-01T11:00:00Z")
        attempts (atom 0)
        app (-> (h/test-client)
                (absurd/register-task! "fixed-backoff"
                                       (fn [_ _]
                                         (when (< (swap! attempts inc) 2)
                                           (throw (ex-info "first-fail" {})))
                                         {:attempts @attempts})))
        _ (h/set-fake-now! h/*conn* base)
        spawned (absurd/spawn! app "fixed-backoff" nil
                               {:max-attempts 2
                                :retry-strategy {:kind "fixed"
                                                 :base-seconds 10}})]
    (work! app)
    (is (= "sleeping" (:state (task app spawned))))
    (h/set-fake-now! h/*conn* (.plusSeconds base 10))
    (work! app)
    (let [t (task app spawned)]
      (is (= "completed" (:state t)))
      (is (= 2 (:attempts t))))))

(deftest task-fails-permanently-after-max-attempts
  (let [app (-> (h/test-client)
                (absurd/register-task! "always-fail" (fn [_ _] (throw (ex-info "always fails" {})))
                                       {:max-attempts 2}))
        spawned (absurd/spawn! app "always-fail" nil)]
    (work! app)
    (is (= "pending" (:state (task app spawned))))
    (work! app)
    (let [t (task app spawned)]
      (is (= "failed" (:state t)))
      (is (= 2 (:attempts t))))))

(deftest registered-task-without-retry-default-uses-client-default
  (let [app (-> (h/test-client {:max-attempts 2})
                (absurd/register-task! "uses-client-default" (fn [_ _] (throw (ex-info "always fails" {})))))
        spawned (absurd/spawn! app "uses-client-default" nil)]
    (is (= 2 (:max-attempts (task app spawned))))
    (work! app)
    (is (= "pending" (:state (task app spawned))))
    (work! app)
    (let [t (task app spawned)]
      (is (= "failed" (:state t)))
      (is (= 2 (:attempts t))))))

(deftest cancellation-by-max-duration
  (let [base (Instant/parse "2024-05-01T09:00:00Z")
        app (-> (h/test-client)
                (absurd/register-task! "duration-cancel" (fn [_ _] (throw (ex-info "always fails" {})))))
        _ (h/set-fake-now! h/*conn* base)
        spawned (absurd/spawn! app "duration-cancel" nil
                               {:max-attempts 4
                                :retry-strategy {:kind :fixed
                                                 :base-seconds 30}
                                :cancellation {:max-duration 90}})]
    (work! app)
    (h/set-fake-now! h/*conn* (.plusSeconds base 91))
    (work! app)
    (let [t (task app spawned)]
      (is (= "cancelled" (:state t)))
      (is (some? (:cancelled-at t))))))

(deftest cancellation-by-max-delay
  (let [base (Instant/parse "2024-05-01T08:00:00Z")
        app (-> (h/test-client)
                (absurd/register-task! "delay-cancel" (fn [_ _] {:done true})))
        _ (h/set-fake-now! h/*conn* base)
        spawned (absurd/spawn! app "delay-cancel" nil {:cancellation {:max-delay 60}})]
    (h/set-fake-now! h/*conn* (.plusSeconds base 61))
    (work! app)
    (let [t (task app spawned)]
      (is (= "cancelled" (:state t)))
      (is (some? (:cancelled-at t))))))

(deftest default-cancellation-applied-at-execution
  (let [base (Instant/parse "2024-05-01T08:00:00Z")
        app (-> (h/test-client)
                (absurd/register-task! "slow" (fn [_ _] {:ok true}) {:cancellation {:max-delay 60}}))
        _ (h/set-fake-now! h/*conn* base)
        spawned (absurd/spawn! app "slow" nil)]
    (h/set-fake-now! h/*conn* (.plusSeconds base 61))
    (work! app)
    (is (= "cancelled" (:state (task app spawned))))))

(deftest manual-cancel-pending-task
  (let [app (-> (h/test-client) (absurd/register-task! "pending-cancel" (fn [_ _] {:ok true})))
        spawned (absurd/spawn! app "pending-cancel" {:data 1})]
    (is (= "pending" (:state (task app spawned))))
    (absurd/cancel-task! app (:task-id spawned))
    (let [t (task app spawned)]
      (is (= "cancelled" (:state t)))
      (is (some? (:cancelled-at t))))
    (is (empty? (absurd/claim-tasks! app {:worker-id "worker-1"
                                          :claim-timeout 60})))))

(deftest manual-cancel-running-task
  (let [app (-> (h/test-client) (absurd/register-task! "running-cancel" (fn [_ _] {:ok true})))
        spawned (absurd/spawn! app "running-cancel" {:data 1})
        claims (absurd/claim-tasks! app {:worker-id "worker-1"
                                         :claim-timeout 60})]
    (is (= 1 (count claims)))
    (is (= (:task-id spawned) (:task-id (first claims))))
    (absurd/cancel-task! app (:task-id spawned))
    (let [t (task app spawned)]
      (is (= "cancelled" (:state t)))
      (is (some? (:cancelled-at t))))))

(deftest cancel-blocks-checkpoint-writes
  (let [app (-> (h/test-client) (absurd/register-task! "checkpoint-cancel" (fn [_ _] {:ok true})))
        spawned (absurd/spawn! app "checkpoint-cancel" {:data 1})
        [claim] (absurd/claim-tasks! app {:worker-id "worker-1"
                                          :claim-timeout 60})]
    (absurd/cancel-task! app (:task-id spawned))
    (let [e (try (store/set-task-checkpoint-state! (:store app) (:queue app) (:task-id spawned) "step-1"
                                                   {:result "value"} (:run-id claim) 60)
                 nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :absurd/cancelled (:type (ex-data e))))
      (is (str/includes? (str/lower-case (ex-message e)) "cancelled")))))

(deftest cancel-is-idempotent
  (let [app (-> (h/test-client) (absurd/register-task! "idempotent-cancel" (fn [_ _] {:ok true})))
        spawned (absurd/spawn! app "idempotent-cancel" {:data 1})]
    (absurd/cancel-task! app (:task-id spawned))
    (let [first-cancel (:cancelled-at (task app spawned))]
      (is (some? first-cancel))
      (absurd/cancel-task! app (:task-id spawned))
      (is (= first-cancel (:cancelled-at (task app spawned)))))))

(deftest cancelling-completed-task-is-noop
  (let [app (-> (h/test-client) (absurd/register-task! "complete-cancel" (fn [_ _] {:status "done"})))
        spawned (absurd/spawn! app "complete-cancel" {:data 1})]
    (work! app)
    (absurd/cancel-task! app (:task-id spawned))
    (let [t (task app spawned)]
      (is (= "completed" (:state t)))
      (is (nil? (:cancelled-at t))))))

(deftest cancelling-failed-task-is-noop
  (let [app (-> (h/test-client)
                (absurd/register-task! "failed-cancel" (fn [_ _] (throw (ex-info "boom" {}))) {:max-attempts 1}))
        spawned (absurd/spawn! app "failed-cancel" {:data 1})]
    (work! app)
    (absurd/cancel-task! app (:task-id spawned))
    (let [t (task app spawned)]
      (is (= "failed" (:state t)))
      (is (nil? (:cancelled-at t))))))

(deftest cancel-non-existent-task-errors
  (let [app (h/test-client)
        e (try (absurd/cancel-task! app "019a32d3-8425-7ae2-a5af-2f17a6707666")
               nil
               (catch java.sql.SQLException e e))]
    (is (some? e))
    (is (str/includes? (ex-message e) "not found"))))
