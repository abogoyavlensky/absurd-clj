(ns absurd.sleep-test
  (:require [absurd.core :as absurd]
            [absurd.test-helpers :as h]
            [clojure.test :refer [deftest is testing use-fixtures]])
  (:import [java.time Instant]))

(use-fixtures :each h/with-conn)

(defn- work! [app]
  (absurd/work-batch! app {:worker-id "worker-sleep"
                           :claim-timeout 120
                           :batch-size 1}))

(defn- available-at ^Instant [run]
  (:available-at run))

(deftest sleep-for-suspends-until-duration-elapses
  (let [base (Instant/parse "2024-05-05T10:00:00Z")
        _ (h/set-fake-now! h/*conn* base)
        app (-> (h/test-client)
                (absurd/register-task! "sleep-for"
                                       (fn [_ ctx]
                                         (absurd/sleep-for ctx "wait-for" 60)
                                         {:resumed true})))
        spawned (absurd/spawn! app "sleep-for" nil)]
    (work! app)
    (let [run (h/get-run h/*conn* (:queue app) (:run-id spawned))]
      (is (= "sleeping" (:state run)))
      (is (= (.plusSeconds base 60) (available-at run))))
    (h/set-fake-now! h/*conn* (.plusSeconds base 65))
    (work! app)
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:resumed true} (:completed-payload task))))))

(deftest sleep-for-schedules-relative-to-database-clock
  (let [process-now (Instant/parse "2024-05-05T10:00:00Z")
        db-now (.plusSeconds process-now 60)
        executions (atom 0)
        app (-> (h/test-client {:now-fn (constantly process-now)})
                (absurd/register-task! "sleep-for-db-clock"
                                       (fn [_ ctx]
                                         (swap! executions inc)
                                         (absurd/sleep-for ctx "wait-for" 10)
                                         {:resumed true})))
        _ (h/set-fake-now! h/*conn* db-now)
        spawned (absurd/spawn! app "sleep-for-db-clock" nil)]
    (work! app)
    (is (= 1 @executions))
    (let [run (h/get-run h/*conn* (:queue app) (:run-id spawned))]
      (is (= "sleeping" (:state run)))
      (is (= (.plusSeconds db-now 10) (available-at run))))
    (testing "the checkpoint keeps the process-clock wake time as an ISO string"
      (is (= "2024-05-05T10:00:10Z"
             (:state (first (h/get-checkpoints h/*conn* (:queue app) (:task-id spawned)))))))
    (work! app)
    (is (= 1 @executions))))

(deftest sleep-until-checkpoint-prevents-rescheduling
  (let [base (Instant/parse "2024-05-06T09:00:00Z")
        wake (.plusSeconds base 300)
        executions (atom 0)
        _ (h/set-fake-now! h/*conn* base)
        app (-> (h/test-client)
                (absurd/register-task! "sleep-until"
                                       (fn [_ ctx]
                                         (swap! executions inc)
                                         (absurd/sleep-until ctx "sleep-step" wake)
                                         {:executions @executions})))
        spawned (absurd/spawn! app "sleep-until" nil)]
    (work! app)
    (let [[checkpoint] (h/get-checkpoints h/*conn* (:queue app) (:task-id spawned))]
      (is (= "sleep-step" (:checkpoint-name checkpoint)))
      (is (= "2024-05-06T09:05:00Z" (:state checkpoint)))
      (is (= (:run-id spawned) (str (:owner-run-id checkpoint)))))
    (is (= "sleeping" (:state (h/get-run h/*conn* (:queue app) (:run-id spawned)))))
    (h/set-fake-now! h/*conn* wake)
    (work! app)
    (let [task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
      (is (= "completed" (:state task)))
      (is (= {:executions 2} (:completed-payload task))))
    (is (= 2 @executions))))

(deftest sync-worker-suspends-until-alarm
  (let [base (Instant/parse "2024-06-01T08:00:00Z")
        _ (h/set-fake-now! h/*conn* base)
        app (-> (h/test-client)
                (absurd/register-task! "remind"
                                       (fn [params ctx]
                                         (absurd/sleep-for ctx "snooze" 3600)
                                         {:reminded (:who params)})))
        spawned (absurd/spawn! app "remind" {:who "me"})]
    (work! app)
    (is (= "sleeping" (:state (h/get-task h/*conn* (:queue app) (:task-id spawned)))))
    (h/set-fake-now! h/*conn* (.plusSeconds base 3599))
    (work! app)
    (is (= "sleeping" (:state (h/get-task h/*conn* (:queue app) (:task-id spawned)))))
    (h/set-fake-now! h/*conn* (.plusSeconds base 3600))
    (work! app)
    (is (= {:state :completed
            :result {:reminded "me"}}
           (absurd/fetch-task-result app (:task-id spawned))))))

(deftest sleep-for-schedules-future-run-from-a-context
  (let [app (h/test-client)
        _ (absurd/spawn! app "snoozer" {} {:queue (:queue app)})
        [claim] (absurd/claim-tasks! app {:worker-id "worker"})
        ctx ((ns-resolve 'absurd.core 'make-context) app claim 90 nil)
        before (Instant/now)]
    (is (= :absurd/suspend (h/ex-type #(absurd/sleep-for ctx "retry" 2))))
    (let [[checkpoint] (h/get-checkpoints h/*conn* (:queue app) (:task-id claim))
          scheduled (Instant/parse (:state checkpoint))
          run (h/get-run h/*conn* (:queue app) (:run-id claim))]
      (is (= "retry" (:checkpoint-name checkpoint)))
      (is (.isAfter scheduled before))
      (is (= "sleeping" (:state run)))
      (is (nil? (:wake-event run)))
      (is (.isAfter (available-at run) before))
      (is (< (Math/abs (- (.toEpochMilli (available-at run)) (.toEpochMilli scheduled))) 1000)))))

(deftest sleep-until-accepts-epoch-seconds-and-dates-and-skips-past-times
  (let [app (h/test-client)
        _ (absurd/spawn! app "past" {} {:queue (:queue app)})
        [claim] (absurd/claim-tasks! app {:worker-id "worker"})
        ctx ((ns-resolve 'absurd.core 'make-context) app claim 90 nil)
        past-epoch (- (quot (System/currentTimeMillis) 1000) 60)]
    (testing "a wake time in the past returns without suspending"
      (is (nil? (absurd/sleep-until ctx "epoch" past-epoch)))
      (is (nil? (absurd/sleep-until ctx "date" (java.util.Date. (* 1000 past-epoch))))))
    (is (= ["date" "epoch"]
           (mapv :checkpoint-name (h/get-checkpoints h/*conn* (:queue app) (:task-id claim)))))
    (is (= "running" (:state (h/get-run h/*conn* (:queue app) (:run-id claim)))))))
