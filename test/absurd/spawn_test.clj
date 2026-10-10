(ns absurd.spawn-test
  (:require [absurd.core :as absurd]
            [absurd.test-helpers :as h]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(use-fixtures :each h/with-conn)

(defn- noop [_params _ctx] {:done true})

(deftest spawn-with-idempotency-key-creates-task
  (let [app (-> (h/test-client) (absurd/register-task! "my-task" noop))
        result (absurd/spawn! app "my-task" {:value 42} {:idempotency-key "unique-key-1"})]
    (is (string? (:task-id result)))
    (is (string? (:run-id result)))
    (is (= 1 (:attempt result)))
    (is (true? (:created result)))
    (let [task (h/get-task h/*conn* (:queue app) (:task-id result))]
      (is (= "my-task" (:task-name task)))
      (is (= "pending" (:state task)))
      (is (= {:value 42} (:params task))))))

(deftest spawn-with-same-idempotency-key-returns-existing-task
  (let [app (-> (h/test-client) (absurd/register-task! "dup-task" noop))
        first-spawn (absurd/spawn! app "dup-task" {:value 1} {:idempotency-key "dup-key"})
        second-spawn (absurd/spawn! app "dup-task" {:value 2} {:idempotency-key "dup-key"})]
    (is (= (:task-id first-spawn) (:task-id second-spawn)))
    (is (= (:run-id first-spawn) (:run-id second-spawn)))
    (is (= (:attempt first-spawn) (:attempt second-spawn)))
    (is (false? (:created second-spawn)))
    (is (= 1 (h/count-tasks h/*conn* (:queue app))))))

(deftest spawn-without-idempotency-key-always-creates-new-task
  (let [app (-> (h/test-client) (absurd/register-task! "no-idem-task" noop))
        a (absurd/spawn! app "no-idem-task" {:value 1})
        b (absurd/spawn! app "no-idem-task" {:value 2})]
    (is (not= (:task-id a) (:task-id b)))
    (is (= 2 (h/count-tasks h/*conn* (:queue app))))))

(deftest different-idempotency-keys-create-separate-tasks
  (let [app (-> (h/test-client) (absurd/register-task! "diff-keys-task" noop))
        a (absurd/spawn! app "diff-keys-task" {:value 1} {:idempotency-key "key-a"})
        b (absurd/spawn! app "diff-keys-task" {:value 2} {:idempotency-key "key-b"})]
    (is (not= (:task-id a) (:task-id b)))
    (is (= 2 (h/count-tasks h/*conn* (:queue app))))))

(deftest spawn-options-parity
  (let [app (-> (h/test-client) (absurd/register-task! "opts" noop))
        spawned (absurd/spawn! app "opts" {:value 42}
                               {:max-attempts 3
                                :headers {:trace_id "abc"}
                                :retry-strategy {:kind :fixed
                                                 :base-seconds 30}
                                :cancellation {:max-delay 60
                                               :max-duration 120}
                                :idempotency-key "idem-1"})
        task (h/get-task h/*conn* (:queue app) (:task-id spawned))]
    (is (true? (:created spawned)))
    (is (= 1 (:attempts task)))
    (is (= 3 (:max-attempts task)))
    (is (= {:trace_id "abc"} (:headers task)))
    (is (= {:kind "fixed"
            :base_seconds 30} (:retry-strategy task)))
    (is (= {:max_delay 60
            :max_duration 120} (:cancellation task)))
    (let [dupe (absurd/spawn! app "opts" {:value 99} {:idempotency-key "idem-1"})]
      (is (= (dissoc spawned :created) (dissoc dupe :created)))
      (is (false? (:created dupe))))
    (is (= 1 (h/count-tasks h/*conn* (:queue app))))))

(deftest header-keys-are-stored-as-written
  (let [app (-> (h/test-client) (absurd/register-task! "hdr" noop))
        spawned (absurd/spawn! app "hdr" nil {:headers {:trace-id "t"
                                                        "snake_key" 1}})]
    (is (= {:trace-id "t"
            :snake_key 1}
           (:headers (h/get-task h/*conn* (:queue app) (:task-id spawned)))))))

(deftest unknown-task-requires-explicit-queue-at-spawn
  (let [app (h/test-client)]
    (is (= :absurd/unregistered-task (h/ex-type #(absurd/spawn! app "ghost" {}))))
    (let [spawned (absurd/spawn! app "ghost" {} {:queue (:queue app)})]
      (is (= "ghost" (:task-name (h/get-task h/*conn* (:queue app) (:task-id spawned))))))))

(deftest registered-task-rejects-mismatched-queue
  (let [app (-> (h/test-client) (absurd/register-task! "fixed" noop))]
    (is (= :absurd/queue-mismatch
           (h/ex-type #(absurd/spawn! app "fixed" {} {:queue "elsewhere"}))))
    (is (some? (absurd/spawn! app "fixed" {} {:queue (:queue app)})))))

(deftest max-attempts-precedence
  (let [app (-> (h/test-client {:max-attempts 9})
                (absurd/register-task! "plain" noop)
                (absurd/register-task! "with-default" noop {:max-attempts 4}))
        attempts (fn [task-name opts]
                   (:max-attempts (h/get-task h/*conn* (:queue app)
                                              (:task-id (absurd/spawn! app task-name nil opts)))))]
    (is (= 9 (attempts "plain" {})))
    (is (= 4 (attempts "with-default" {})))
    (is (= 2 (attempts "with-default" {:max-attempts 2})))
    (is (= 9 (attempts "unregistered" {:queue (:queue app)})))))

(deftest default-cancellation-from-registration-is-applied
  (let [app (-> (h/test-client)
                (absurd/register-task! "slow" noop {:cancellation {:max-delay 60}}))
        spawned (absurd/spawn! app "slow" nil)
        explicit (absurd/spawn! app "slow" nil {:cancellation {:max-duration 5}})]
    (is (= {:max_delay 60} (:cancellation (h/get-task h/*conn* (:queue app) (:task-id spawned)))))
    (is (= {:max_duration 5} (:cancellation (h/get-task h/*conn* (:queue app) (:task-id explicit)))))))

(deftest before-spawn-hook-can-inject-and-preserve-headers
  (let [seen (atom [])
        hook (fn [task-name params opts]
               (swap! seen conj [task-name params])
               (update opts :headers merge {:injected "by-hook"}))
        app (-> (h/test-client {:hooks {:before-spawn hook}})
                (absurd/register-task! "merge-headers" noop))
        spawned (absurd/spawn! app "merge-headers" {:p 1} {:headers {:existing "user-provided"}})]
    (is (= [["merge-headers" {:p 1}]] @seen))
    (is (= {:existing "user-provided"
            :injected "by-hook"}
           (:headers (h/get-task h/*conn* (:queue app) (:task-id spawned)))))))

(deftest cancel-pending-task
  (let [app (-> (h/test-client) (absurd/register-task! "c" noop))
        spawned (absurd/spawn! app "c" nil)]
    (absurd/cancel-task! app (:task-id spawned))
    (is (= "cancelled" (:state (h/get-task h/*conn* (:queue app) (:task-id spawned)))))
    (is (= {:state :cancelled} (absurd/fetch-task-result app (:task-id spawned))))))

(deftest fetch-and-await-task-result-without-a-worker
  (let [app (-> (h/test-client) (absurd/register-task! "r" noop))
        spawned (absurd/spawn! app "r" nil)]
    (is (nil? (absurd/fetch-task-result app (str (random-uuid)))))
    (is (= {:state :pending} (absurd/fetch-task-result app (:task-id spawned))))
    (testing "timeout is reported as :absurd/timeout"
      (is (= :absurd/timeout
             (h/ex-type #(absurd/await-task-result app (:task-id spawned) {:timeout 0.2})))))
    (testing "negative timeout times out immediately"
      (let [started (System/nanoTime)]
        (is (= :absurd/timeout
               (h/ex-type #(absurd/await-task-result app (:task-id spawned) {:timeout -5}))))
        (is (< (/ (- (System/nanoTime) started) 1e6) 500))))
    (testing "unknown task is reported as not found"
      (is (= :absurd/task-not-found
             (h/ex-type #(absurd/await-task-result app (str (random-uuid)) {:timeout 1})))))))

(deftest emit-event-validates-name
  (let [app (h/test-client)]
    (is (= :absurd/invalid-event-name (h/ex-type #(absurd/emit-event! app "" {:x 1}))))
    (is (nil? (absurd/emit-event! app "ok" {:x 1})))
    (is (nil? (absurd/emit-event! app "ok-other-queue" nil {:queue (:queue app)})))))
