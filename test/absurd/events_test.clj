(ns absurd.events-test
  (:require [absurd.core :as absurd]
            [absurd.test-helpers :as h]
            [clojure.test :refer [deftest is testing use-fixtures]])
  (:import [java.time Instant]))

(use-fixtures :each h/with-conn)

(defn- work!
  ([app] (work! app 1))
  ([app batch-size] (absurd/work-batch! app {:worker-id "worker1"
                                             :claim-timeout 120
                                             :batch-size batch-size})))

(defn- available-at ^Instant [run]
  (:available-at run))

(deftest await-and-emit-event-flow
  (let [app (h/test-client)
        event-name (str "test_event_" (:queue app))
        _ (absurd/register-task! app "waiter"
                                 (fn [_ ctx] {:received (absurd/await-event ctx event-name {:timeout 60})}))
        spawned (absurd/spawn! app "waiter" {:step 1})]
    (work! app)
    (let [run (h/get-run h/*conn* (:queue app) (:run-id spawned))]
      (is (= "sleeping" (:state run)))
      (is (= event-name (:wake-event run))))
    (absurd/emit-event! app event-name {:value 42})
    (is (= "pending" (:state (h/get-run h/*conn* (:queue app) (:run-id spawned)))))
    (work! app)
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:received {:value 42}} (:completed-payload task))))))

(deftest event-emitted-before-await-is-cached
  (let [app (h/test-client)
        event-name (str "cached_event_" (:queue app))
        _ (absurd/emit-event! app event-name {:data "pre-emitted"})
        _ (absurd/register-task! app "late-waiter"
                                 (fn [_ ctx] {:received (absurd/await-event ctx event-name)}))
        spawned (absurd/spawn! app "late-waiter" nil)]
    (work! app)
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:received {:data "pre-emitted"}} (:completed-payload task))))))

(deftest emit-event-is-first-write-wins
  (let [app (h/test-client)
        event-name (str "stable_event_" (:queue app))
        first-emit (Instant/parse "2024-05-01T09:00:00Z")]
    (h/set-fake-now! h/*conn* first-emit)
    (absurd/emit-event! app event-name {:value 1})
    (h/set-fake-now! h/*conn* (.plusSeconds first-emit 30))
    (absurd/emit-event! app event-name {:value 2})
    (let [row (h/get-event h/*conn* (:queue app) event-name)]
      (is (= {:value 1} (:payload row)))
      (is (= first-emit (:emitted-at row))))
    (absurd/register-task! app "late-waiter" (fn [_ ctx] {:received (absurd/await-event ctx event-name)}))
    (let [spawned (absurd/spawn! app "late-waiter" nil)]
      (work! app)
      (is (= {:received {:value 1}}
             (:completed-payload (h/get-task h/*conn* (:queue app) (:task-id spawned))))))))

(deftest await-event-with-timeout-expires-and-wakes-task
  (let [app (h/test-client)
        event-name (str "timeout_event_" (:queue app))
        base (Instant/parse "2024-05-01T10:00:00Z")
        _ (h/set-fake-now! h/*conn* base)
        _ (absurd/register-task! app "timeout-waiter"
                                 (fn [_ ctx]
                                   (try
                                     {:timedOut false
                                      :result (absurd/await-event ctx event-name {:timeout 600})}
                                     (catch clojure.lang.ExceptionInfo e
                                       (if (= :absurd/timeout (:type (ex-data e)))
                                         {:timedOut true
                                          :result nil}
                                         (throw e))))))
        spawned (absurd/spawn! app "timeout-waiter" nil)]
    (work! app)
    (is (= 1 (h/count-waits h/*conn* (:queue app))))
    (let [run (h/get-run h/*conn* (:queue app) (:run-id spawned))]
      (is (= "sleeping" (:state run)))
      (is (= event-name (:wake-event run)))
      (is (= (.plusSeconds base 600) (available-at run))))
    (h/set-fake-now! h/*conn* (.plusSeconds base 601))
    (work! app)
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:timedOut true
              :result nil} (:completed-payload task))))
    (is (= 0 (h/count-waits h/*conn* (:queue app))))))

(deftest multiple-tasks-can-await-same-event
  (let [app (h/test-client)
        event-name (str "broadcast_event_" (:queue app))
        _ (absurd/register-task! app "multi-waiter"
                                 (fn [params ctx]
                                   {:taskNum (:taskNum params)
                                    :received (absurd/await-event ctx event-name)}))
        tasks (mapv #(absurd/spawn! app "multi-waiter" {:taskNum %}) [1 2 3])]
    (work! app 10)
    (doseq [spawned tasks]
      (is (= "sleeping" (:state (h/get-task h/*conn* (:queue app) (:task-id spawned))))))
    (absurd/emit-event! app event-name {:data "broadcast"})
    (work! app 10)
    (doseq [[i spawned] (map-indexed vector tasks)]
      (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
        (is (= "completed" (:state task)))
        (is (= {:taskNum (inc i)
                :received {:data "broadcast"}} (:completed-payload task)))))))

(deftest await-event-timeout-does-not-recreate-wait-on-resume
  (let [app (h/test-client)
        event-name (str "timeout_no_loop_" (:queue app))
        base (Instant/parse "2024-05-02T11:00:00Z")
        _ (h/set-fake-now! h/*conn* base)
        _ (absurd/register-task! app "timeout-no-loop"
                                 (fn [_ ctx]
                                   (try
                                     (absurd/await-event ctx event-name {:step-name "wait"
                                                                         :timeout 10})
                                     {:stage "unexpected"}
                                     (catch clojure.lang.ExceptionInfo e
                                       (if (= :absurd/timeout (:type (ex-data e)))
                                         {:stage "resumed"
                                          :payload (absurd/await-event ctx event-name {:step-name "wait"
                                                                                       :timeout 10})}
                                         (throw e))))))
        spawned (absurd/spawn! app "timeout-no-loop" nil)]
    (work! app)
    (is (= 1 (h/count-waits h/*conn* (:queue app))))
    (h/set-fake-now! h/*conn* (.plusSeconds base 15))
    (work! app)
    (is (= 0 (h/count-waits h/*conn* (:queue app))))
    (is (= "completed" (:state (h/get-run h/*conn* (:queue app) (:run-id spawned)))))
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:stage "resumed"
              :payload nil} (:completed-payload task))))))

(deftest await-event-flow-from-a-context
  (let [app (h/test-client)
        make-context (ns-resolve 'absurd.core 'make-context)
        _ (absurd/spawn! app "waiter" {} {:queue (:queue app)})
        [claim] (absurd/claim-tasks! app {:worker-id "worker"})
        ctx (make-context app claim 120 nil)
        event-name "user.ready"]
    (is (= :absurd/suspend (h/ex-type #(absurd/await-event ctx event-name {:step-name "wait"}))))
    (let [run (h/get-run h/*conn* (:queue app) (:run-id claim))]
      (is (= "sleeping" (:state run)))
      (is (= event-name (:wake-event run))))
    (absurd/emit-event! app event-name {:status "ok"} {:queue (:queue app)})
    (let [[replay] (absurd/claim-tasks! app {:worker-id "worker"})
          resumed (make-context app replay 120 nil)]
      (is (= {:status "ok"} (absurd/await-event resumed event-name {:step-name "wait"})))
      (is (= [{:checkpoint-name "wait"
               :state {:status "ok"}}]
             (mapv #(select-keys % [:checkpoint-name :state])
                   (h/get-checkpoints h/*conn* (:queue app) (:task-id replay))))))
    (testing "the context can emit events on its own queue"
      (absurd/emit-event! ctx "from-ctx" {:n 1})
      (is (= {:n 1} (:payload (h/get-event h/*conn* (:queue app) "from-ctx")))))))

(deftest await-event-negative-timeout-does-not-sleep-forever
  (let [app (-> (h/test-client)
                (absurd/register-task! "wait-negative-timeout"
                                       (fn [_ ctx]
                                         (try
                                           (absurd/await-event ctx "never-emitted" {:timeout -1})
                                           {:timed_out false}
                                           (catch clojure.lang.ExceptionInfo e
                                             (if (= :absurd/timeout (:type (ex-data e)))
                                               {:timed_out true}
                                               (throw e)))))))
        spawned (absurd/spawn! app "wait-negative-timeout" nil)]
    (work! app)
    (work! app)
    (is (= {:state :completed
            :result {:timed_out true}}
           (absurd/await-task-result app (:task-id spawned) {:timeout 1})))))

(deftest await-event-validates-name
  (let [app (h/test-client)
        _ (absurd/spawn! app "x" {} {:queue (:queue app)})
        [claim] (absurd/claim-tasks! app {:worker-id "worker"})
        ctx ((ns-resolve 'absurd.core 'make-context) app claim 120 nil)]
    (is (= :absurd/invalid-event-name (h/ex-type #(absurd/await-event ctx ""))))))
