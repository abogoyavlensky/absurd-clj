(ns absurd.step-test
  (:require [absurd.core :as absurd]
            [absurd.test-helpers :as h]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(use-fixtures :each h/with-conn)

(defn- work! [app]
  (absurd/work-batch! app {:worker-id "worker"
                           :claim-timeout 60
                           :batch-size 1}))

(deftest step-executes-and-returns-value
  (let [app (-> (h/test-client)
                (absurd/register-task! "basic"
                                       (fn [params ctx]
                                         {:result (absurd/step ctx "process"
                                                               #(str "processed-" (:value params)))})))
        spawned (absurd/spawn! app "basic" {:value 42})]
    (work! app)
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:result "processed-42"} (:completed-payload task))))))

(deftest step-result-is-cached-and-not-reexecuted-on-retry
  (let [executions (atom 0)
        attempts (atom 0)
        app (-> (h/test-client)
                (absurd/register-task! "cache"
                                       (fn [_ ctx]
                                         (swap! attempts inc)
                                         (let [cached (absurd/step ctx "generate-random"
                                                                   (fn [] (swap! executions inc) 42))]
                                           (when (= 1 @attempts)
                                             (throw (ex-info "Intentional failure" {})))
                                           {:random cached
                                            :count @executions}))
                                       {:max-attempts 2}))
        spawned (absurd/spawn! app "cache" nil)]
    (work! app)
    (is (= 1 @executions))
    (work! app)
    (is (= 1 @executions))
    (is (= 2 @attempts))
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= 1 (get-in task [:completed-payload :count])))
      (is (= 2 (:attempts task))))))

(deftest begin-complete-step-result-is-cached-and-not-reexecuted-on-retry
  (let [executions (atom 0)
        attempts (atom 0)
        app (-> (h/test-client)
                (absurd/register-task! "cache-decomposed"
                                       (fn [_ ctx]
                                         (swap! attempts inc)
                                         (let [handle (absurd/begin-step ctx "generate")
                                               value (if (:done? handle)
                                                       (:state handle)
                                                       (do (swap! executions inc)
                                                           (absurd/complete-step ctx handle 42)))]
                                           (when (= 1 @attempts)
                                             (throw (ex-info "Intentional failure" {})))
                                           {:value value
                                            :count @executions}))
                                       {:max-attempts 2}))
        spawned (absurd/spawn! app "cache-decomposed" nil)]
    (work! app)
    (is (= 1 @executions))
    (work! app)
    (is (= 1 @executions))
    (is (= 2 @attempts))
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:value 42
              :count 1} (:completed-payload task)))
      (is (= 2 (:attempts task))))))

(deftest multistep-task-only-reexecutes-uncompleted-steps
  (let [executed (atom [])
        attempts (atom 0)
        run-step (fn [ctx step-name result]
                   (absurd/step ctx step-name (fn [] (swap! executed conj step-name) result)))
        app (-> (h/test-client)
                (absurd/register-task! "multistep-retry"
                                       (fn [_ ctx]
                                         (swap! attempts inc)
                                         (let [s1 (run-step ctx "step1" "result1")
                                               s2 (run-step ctx "step2" "result2")]
                                           (when (= 1 @attempts)
                                             (throw (ex-info "Fail before step3" {})))
                                           (let [s3 (run-step ctx "step3" "result3")]
                                             {:steps [s1 s2 s3]
                                              :attemptNum @attempts})))
                                       {:max-attempts 2}))
        spawned (absurd/spawn! app "multistep-retry" nil)]
    (work! app)
    (is (= ["step1" "step2"] @executed))
    (work! app)
    (is (= ["step1" "step2" "step3"] @executed))
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:steps ["result1" "result2" "result3"]
              :attemptNum 2} (:completed-payload task)))
      (is (= 2 (:attempts task))))))

(deftest repeated-step-names-work-correctly
  (let [app (-> (h/test-client)
                (absurd/register-task! "deduplicate"
                                       (fn [_ ctx]
                                         {:results (mapv (fn [i] (absurd/step ctx "loop-step" #(* i 10)))
                                                         (range 3))})))
        spawned (absurd/spawn! app "deduplicate" nil)]
    (work! app)
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:results [0 10 20]} (:completed-payload task))))
    (is (= ["loop-step" "loop-step#2" "loop-step#3"]
           (mapv :checkpoint-name (h/get-checkpoints h/*conn* (:queue app) (:task-id spawned)))))))

(deftest repeated-begin-complete-step-names-auto-number-correctly
  (let [app (-> (h/test-client)
                (absurd/register-task! "deduplicate-decomposed"
                                       (fn [_ ctx]
                                         {:results (mapv (fn [i]
                                                           (let [handle (absurd/begin-step ctx "loop-step")]
                                                             (if (:done? handle)
                                                               (:state handle)
                                                               (absurd/complete-step ctx handle (* i 10)))))
                                                         (range 3))})))
        spawned (absurd/spawn! app "deduplicate-decomposed" nil)]
    (work! app)
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:results [0 10 20]} (:completed-payload task))))))

(deftest failed-step-does-not-save-checkpoint-and-reexecutes
  (let [attempts (atom 0)
        app (-> (h/test-client)
                (absurd/register-task! "fail"
                                       (fn [_ ctx]
                                         {:result (absurd/step ctx "fail"
                                                               (fn []
                                                                 (swap! attempts inc)
                                                                 (when (= 1 @attempts)
                                                                   (throw (ex-info "Step fails on first attempt" {})))
                                                                 "success"))})
                                       {:max-attempts 2}))
        spawned (absurd/spawn! app "fail" nil)]
    (work! app)
    (is (= 1 @attempts))
    (is (= 0 (h/count-checkpoints h/*conn* (:queue app) (:task-id spawned))))
    (work! app)
    (is (= 2 @attempts))
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:result "success"} (:completed-payload task)))
      (is (= 2 (:attempts task))))))

(deftest step-persists-and-reuses-checkpoints
  (let [app (h/test-client)
        _ (absurd/spawn! app "profile-step" {:user-id 7} {:queue (:queue app)})
        [claim] (absurd/claim-tasks! app {:worker-id "worker"})
        make-context (ns-resolve 'absurd.core 'make-context)
        ctx (make-context app claim 120 nil)
        calls (atom 0)
        compute (fn [] (swap! calls inc) {:user-id (get-in claim [:params :user-id])})]
    (is (= {:user-id 7} (absurd/step ctx "load-profile" compute)))
    (is (= 1 @calls))
    (is (= [{:checkpoint-name "load-profile"
             :state {:user-id 7}}]
           (mapv #(select-keys % [:checkpoint-name :state])
                 (h/get-checkpoints h/*conn* (:queue app) (:task-id claim)))))
    (testing "a brand new context observes the persisted checkpoint"
      (let [resumed (make-context app claim 120 nil)]
        (is (= {:user-id 7} (absurd/step resumed "load-profile" compute)))
        (is (= 1 @calls))))
    (testing "the context map exposes task metadata with bare keys"
      (is (= (:task-id claim) (:task-id ctx)))
      (is (= (:run-id claim) (:run-id ctx)))
      (is (= "profile-step" (:task-name ctx)))
      (is (= 1 (:attempt ctx)))
      (is (= (:queue app) (:queue ctx)))
      (is (= {} (:headers ctx))))))

(deftest step-values-round-trip-through-json-on-first-run
  (let [seen (atom nil)
        app (-> (h/test-client)
                (absurd/register-task! "shapes"
                                       (fn [_ ctx]
                                         (reset! seen (absurd/step ctx "shape" (fn [] {:k :v
                                                                                       :s #{1}
                                                                                       :n nil})))
                                         nil)))]
    (absurd/spawn! app "shapes" nil)
    (work! app)
    (is (= {:k "v"
            :s [1]
            :n nil} @seen))))
