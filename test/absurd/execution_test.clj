(ns absurd.execution-test
  (:require [absurd.core :as absurd]
            [absurd.test-helpers :as h]
            [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]]
            [next.jdbc :as jdbc])
  (:import [java.time Instant]))

(use-fixtures :each h/with-conn)

(defn- work!
  ([app] (work! app "worker"))
  ([app worker-id] (absurd/work-batch! app {:worker-id worker-id
                                            :claim-timeout 60
                                            :batch-size 1})))

(deftest sync-worker-processes-task
  (let [seen (atom [])
        app (-> (h/test-client)
                (absurd/register-task! "send_welcome"
                                       (fn [params ctx]
                                         (let [profile (absurd/step ctx "load-profile"
                                                                    (fn [] {:user_id (:user_id params)}))]
                                           (swap! seen conj profile)
                                           {:status "sent"
                                            :user_id (:user_id params)}))))
        spawned (absurd/spawn! app "send_welcome" {:user_id 42})]
    (work! app "sync-worker")
    (let [run (h/get-run h/*conn* (:queue app) (:run-id spawned))]
      (is (= "completed" (:state run)))
      (is (= {:status "sent"
              :user_id 42} (:result run))))
    (is (= [{:user_id 42}] @seen))))

(deftest unknown-task-is-deferred-not-failed
  (let [app (h/test-client)
        base (Instant/parse "2024-04-01T10:00:00Z")]
    (h/set-fake-now! h/*conn* base)
    (let [spawned (absurd/spawn! app "ghost-task" {:value 1} {:queue (:queue app)
                                                              :max-attempts 1})]
      (work! app "unknown-worker")
      (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))
            run (h/get-run h/*conn* (:queue app) (:run-id spawned))
            available-at (.toInstant ^java.sql.Timestamp (:available-at run))]
        (is (= "sleeping" (:state task)))
        (is (= 1 (:attempts task)))
        (is (= "sleeping" (:state run)))
        (is (nil? (:failure-reason run)))
        (is (<= 15 (- (.getEpochSecond available-at) (.getEpochSecond base)) 30))))))

(deftest unknown-task-defer-failure-preserves-error-context
  (let [app (h/test-client)
        spawned (absurd/spawn! app "ghost-task" {:value 1} {:queue (:queue app)
                                                            :max-attempts 1})]
    (with-redefs-fn {(ns-resolve 'absurd.core 'unknown-task-defer-seconds) (fn [_] "oops")}
      #(work! app "unknown-worker"))
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))
          run (h/get-run h/*conn* (:queue app) (:run-id spawned))]
      (is (= "failed" (:state task)))
      (is (= 1 (:attempts task)))
      (is (= "failed" (:state run)))
      (is (= "ClassCastException" (get-in run [:failure-reason :name])))
      (is (str/includes? (get-in run [:failure-reason :message]) "java.lang.String")))))

(deftest queue-mismatch-fails-claimed-run-immediately
  (let [other-queue (h/random-queue "other")
        worker (h/test-client)
        _ (absurd/create-queue! worker other-queue)
        _ (absurd/register-task! worker "misqueued-task" (fn [_ _] {:ok true})
                                 {:queue other-queue
                                  :max-attempts 1})
        producer (absurd/client {:db h/*conn*
                                 :queue (:queue worker)})
        spawned (absurd/spawn! producer "misqueued-task" nil {:queue (:queue worker)
                                                              :max-attempts 1})]
    (work! worker)
    (let [task (h/get-task h/*conn* (:queue worker) (:task-id spawned))]
      (is (= "failed" (:state task)))
      (is (= 1 (:attempts task))))
    (is (str/includes? (:message (h/get-failure h/*conn* (:queue worker) (:run-id spawned)))
                       "queue mismatch"))))

(deftest failure-includes-traceback
  (let [app (-> (h/test-client)
                (absurd/register-task! "boom" (fn [_ _] (throw (RuntimeException. "boom")))
                                       {:max-attempts 1}))
        spawned (absurd/spawn! app "boom" nil)]
    (work! app)
    (let [failure (h/get-failure h/*conn* (:queue app) (:run-id spawned))]
      (is (= "RuntimeException" (:name failure)))
      (is (= "boom" (:message failure)))
      (is (str/includes? (:traceback failure) "absurd.execution_test")))))

(deftest ex-info-failures-serialize-name-and-message
  (let [app (-> (h/test-client)
                (absurd/register-task! "info" (fn [_ _] (throw (ex-info "with data" {:k 1})))
                                       {:max-attempts 1}))
        spawned (absurd/spawn! app "info" nil)]
    (work! app)
    (let [failure (h/get-failure h/*conn* (:queue app) (:run-id spawned))]
      (is (= "ExceptionInfo" (:name failure)))
      (is (= "with data" (:message failure))))))

(deftest errors-not-only-exceptions-fail-the-run
  (let [app (-> (h/test-client)
                (absurd/register-task! "assert" (fn [_ _] (throw (AssertionError. "nope")))
                                       {:max-attempts 1}))
        spawned (absurd/spawn! app "assert" nil)]
    (work! app)
    (is (= "failed" (:state (h/get-task h/*conn* (:queue app) (:task-id spawned)))))
    (is (= "AssertionError" (:name (h/get-failure h/*conn* (:queue app) (:run-id spawned)))))))

(deftest invalid-headers-fail-run
  (let [app (-> (h/test-client)
                (absurd/register-task! "headered" (fn [_ ctx] {:headers (:headers ctx)})
                                       {:max-attempts 1}))
        spawned (absurd/spawn! app "headered" nil)]
    (jdbc/execute! h/*conn* [(str "update absurd.t_" (:queue app)
                                  " set headers = '\"not-an-object\"'::jsonb where task_id = ?::uuid")
                             (:task-id spawned)])
    (work! app)
    (is (= "failed" (:state (h/get-task h/*conn* (:queue app) (:task-id spawned)))))
    (is (str/includes? (:message (h/get-failure h/*conn* (:queue app) (:run-id spawned)))
                       "invalid task headers"))))

(deftest before-spawn-headers-are-visible-in-the-context
  (let [captured (atom [])
        hook (fn [_ _ opts]
               (update opts :headers merge {:trace_id "trace-123"
                                            :correlation_id "corr-456"}))
        app (-> (h/test-client {:hooks {:before-spawn hook}})
                (absurd/register-task! "capture-headers"
                                       (fn [_ ctx] (swap! captured conj (:headers ctx)) "done")))]
    (absurd/spawn! app "capture-headers" {:test true})
    (work! app "worker1")
    (is (= [{:trace_id "trace-123"
             :correlation_id "corr-456"}] @captured))))

(deftest wrap-task-execution-hook-wraps-the-handler
  (let [order (atom [])
        wrap (fn [_ctx execute]
               (swap! order conj "before")
               (let [result (execute)]
                 (swap! order conj "after")
                 result))
        app (-> (h/test-client {:hooks {:wrap-task-execution wrap}})
                (absurd/register-task! "wrapped-task"
                                       (fn [_ _] (swap! order conj "handler") "done")))
        spawned (absurd/spawn! app "wrapped-task" {})]
    (work! app "worker1")
    (is (= ["before" "handler" "after"] @order))
    (is (= "done" (:result (h/get-run h/*conn* (:queue app) (:run-id spawned)))))))

(def ^:dynamic *trace* nil)

(deftest hooks-propagate-dynamic-context-through-headers
  (let [captured (atom [])
        before-spawn (fn [_ _ opts]
                       (if-let [{:keys [trace-id span-id]} *trace*]
                         (update opts :headers merge {:trace_id trace-id
                                                      :span_id span-id})
                         opts))
        wrap (fn [ctx execute]
               (let [{:keys [trace_id span_id]} (:headers ctx)]
                 (if (and trace_id span_id)
                   (binding [*trace* {:trace-id trace_id
                                      :span-id span_id}]
                     (execute))
                   (execute))))
        app (-> (h/test-client {:hooks {:before-spawn before-spawn
                                        :wrap-task-execution wrap}})
                (absurd/register-task! "contextvar-test"
                                       (fn [_ _] (swap! captured conj *trace*) "done")))]
    (binding [*trace* {:trace-id "trace-abc"
                       :span-id "span-xyz"}]
      (absurd/spawn! app "contextvar-test" {}))
    (work! app "worker1")
    (is (= [{:trace-id "trace-abc"
             :span-id "span-xyz"}] @captured))))

(deftest wrap-hook-that-throws-fails-the-run
  (let [app (-> (h/test-client {:hooks {:wrap-task-execution (fn [_ _] (throw (RuntimeException. "hook broke")))}})
                (absurd/register-task! "wrapped" (fn [_ _] "unreachable") {:max-attempts 1}))
        spawned (absurd/spawn! app "wrapped" {})]
    (work! app)
    (is (= "failed" (:state (h/get-task h/*conn* (:queue app) (:task-id spawned)))))
    (is (= "hook broke" (:message (h/get-failure h/*conn* (:queue app) (:run-id spawned)))))))

(deftest current-context-is-bound-inside-handlers
  (let [seen (atom nil)
        app (-> (h/test-client)
                (absurd/register-task! "ctx" (fn [_ ctx]
                                               (reset! seen [(absurd/current-context) ctx])
                                               nil)))]
    (absurd/spawn! app "ctx" nil)
    (work! app)
    (is (nil? (absurd/current-context)))
    (let [[current ctx] @seen]
      (is (= (:task-id ctx) (:task-id current)))
      (is (= (:run-id ctx) (:run-id current))))))

(deftest nil-and-scalar-results-complete-the-run
  (let [app (-> (h/test-client)
                (absurd/register-task! "nil-result" (fn [_ _] nil))
                (absurd/register-task! "scalar-result" (fn [_ _] 42)))
        a (absurd/spawn! app "nil-result" nil)
        b (absurd/spawn! app "scalar-result" nil)]
    (absurd/work-batch! app {:worker-id "w"
                             :claim-timeout 60
                             :batch-size 2})
    (is (= {:state :completed
            :result nil} (absurd/fetch-task-result app (:task-id a))))
    (is (= {:state :completed
            :result 42} (absurd/fetch-task-result app (:task-id b))))))

(deftest idempotency-key-persists-after-task-completes
  (let [app (-> (h/test-client) (absurd/register-task! "complete-task" (fn [_ _] {:result "done"})))
        first-spawn (absurd/spawn! app "complete-task" {:value 1} {:idempotency-key "complete-key"})]
    (work! app "test-worker")
    (is (= "completed" (:state (h/get-task h/*conn* (:queue app) (:task-id first-spawn)))))
    (let [second-spawn (absurd/spawn! app "complete-task" {:value 2} {:idempotency-key "complete-key"})]
      (is (= (:task-id first-spawn) (:task-id second-spawn)))
      (is (= (:run-id first-spawn) (:run-id second-spawn))))))

(deftest idempotent-spawn-executes-only-once
  (let [executions (atom 0)
        app (-> (h/test-client)
                (absurd/register-task! "fire-forget-task" (fn [_ _] (swap! executions inc) {:done true})))]
    (dotimes [_ 3]
      (absurd/spawn! app "fire-forget-task" {} {:idempotency-key "daily-report:2025-01-15"}))
    (absurd/work-batch! app {:worker-id "test-worker"
                             :batch-size 10})
    (is (= 1 @executions))))

(deftest retry-task-defaults-to-one-more-attempt
  (let [app (-> (h/test-client)
                (absurd/register-task! "always-fail" (fn [_ _] (throw (ex-info "boom" {})))
                                       {:max-attempts 1}))
        spawned (absurd/spawn! app "always-fail" {:payload 1})]
    (work! app)
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "failed" (:state task)))
      (is (= 1 (:attempts task))))
    (let [retry (absurd/retry-task! app (:task-id spawned))]
      (is (= (:task-id spawned) (:task-id retry)))
      (is (= 2 (:attempt retry)))
      (is (false? (:created retry))))
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "pending" (:state task)))
      (is (= 2 (:attempts task))))))

(deftest retry-task-spawn-new-creates-fresh-task
  (let [app (-> (h/test-client)
                (absurd/register-task! "checkpoint-then-fail"
                                       (fn [_ ctx]
                                         (absurd/step ctx "step-1" (fn [] {:ok true}))
                                         (throw (ex-info "boom" {})))
                                       {:max-attempts 1}))
        spawned (absurd/spawn! app "checkpoint-then-fail" {:payload 1})]
    (work! app)
    (is (= 1 (h/count-checkpoints h/*conn* (:queue app) (:task-id spawned))))
    (let [retry (absurd/retry-task! app (:task-id spawned) {:spawn-new? true})]
      (is (true? (:created retry)))
      (is (= 1 (:attempt retry)))
      (is (not= (:task-id spawned) (:task-id retry)))
      (is (= "failed" (:state (h/get-task h/*conn* (:queue app) (:task-id spawned)))))
      (is (= "pending" (:state (h/get-task h/*conn* (:queue app) (:task-id retry)))))
      (is (= 0 (h/count-checkpoints h/*conn* (:queue app) (:task-id retry)))))))
