(ns absurd.worker-test
  (:require [absurd.core :as absurd]
            [absurd.test-helpers :as h]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(use-fixtures :each h/with-conn)

(defn- pooled-client
  "A client on a DataSource (worker threads need their own connections) whose
  queue already exists on the shared connection."
  ([] (pooled-client {}))
  ([opts]
   (let [queue (h/random-queue "worker")]
     (absurd/create-queue! (absurd/client {:db h/*conn*
                                           :queue queue}))
     (absurd/client (merge {:db (h/datasource)
                            :queue queue} opts)))))

(defn- await-latch [^CountDownLatch latch seconds]
  (.await latch (long seconds) TimeUnit/SECONDS))

(defn- wait-until
  "Poll `pred` for up to `seconds`; returns true when it became truthy."
  [pred seconds]
  (let [deadline (+ (System/nanoTime) (* seconds 1e9))]
    (loop []
      (cond
        (pred) true
        (> (System/nanoTime) deadline) false
        :else (do (Thread/sleep 50) (recur))))))

(deftest start-worker-uses-threads-for-concurrency
  (let [app (pooled-client)
        both-started (CountDownLatch. 2)
        release (CountDownLatch. 1)
        running (atom 0)
        max-running (atom 0)
        _ (absurd/register-task! app "threaded-task"
                                 (fn [params _]
                                   (swap! max-running max (swap! running inc))
                                   (.countDown both-started)
                                   (await-latch release 5)
                                   (swap! running dec)
                                   params))
        first-spawn (absurd/spawn! app "threaded-task" {:idx 1})
        second-spawn (absurd/spawn! app "threaded-task" {:idx 2})
        worker (absurd/start-worker! app {:worker-id "sync-threaded-worker"
                                          :claim-timeout 120
                                          :concurrency 2
                                          :poll-interval 10})]
    (try
      (is (await-latch both-started 3))
      (.countDown release)
      (is (wait-until #(every? (fn [spawned]
                                 (= :completed (:state (absurd/fetch-task-result app (:task-id spawned)))))
                               [first-spawn second-spawn])
                      5))
      (is (>= @max-running 2))
      (finally
        (.countDown release)
        (is (true? (absurd/stop-worker! worker)))))
    (is (false? (:running? @worker)))))

(deftest start-worker-keeps-claiming-while-slow-task-runs
  (let [app (pooled-client)
        slow-started (CountDownLatch. 1)
        release-slow (CountDownLatch. 1)
        two-fast-done (CountDownLatch. 2)
        _ (absurd/register-task! app "mixed-speed-task"
                                 (fn [params _]
                                   (if (= "slow" (:kind params))
                                     (do (.countDown slow-started)
                                         (await-latch release-slow 5))
                                     (.countDown two-fast-done))
                                   params))
        slow (absurd/spawn! app "mixed-speed-task" {:kind "slow"
                                                    :idx 0})
        worker (absurd/start-worker! app {:worker-id "sync-threaded-slow-worker"
                                          :claim-timeout 120
                                          :concurrency 2
                                          :poll-interval 10})]
    (try
      (is (await-latch slow-started 3))
      (let [fast1 (absurd/spawn! app "mixed-speed-task" {:kind "fast"
                                                         :idx 1})
            fast2 (absurd/spawn! app "mixed-speed-task" {:kind "fast"
                                                         :idx 2})]
        (is (await-latch two-fast-done 3))
        (is (wait-until #(and (= :completed (:state (absurd/fetch-task-result app (:task-id fast1))))
                              (= :completed (:state (absurd/fetch-task-result app (:task-id fast2)))))
                        3))
        (is (= :running (:state (absurd/fetch-task-result app (:task-id slow))))))
      (finally
        (.countDown release-slow)
        (absurd/stop-worker! worker)))
    (is (= :completed (:state (absurd/fetch-task-result app (:task-id slow)))))))

(deftest worker-recovers-from-handler-errors
  (let [app (-> (pooled-client)
                (absurd/register-task! "panic-task" (fn [_ _] (throw (Error. "boom"))) {:max-attempts 1})
                (absurd/register-task! "ok-task" (fn [_ _] {:ok true}) {:max-attempts 1}))
        worker (absurd/start-worker! app {:worker-id "panic-worker"
                                          :concurrency 1
                                          :poll-interval 25})]
    (try
      (let [panicked (absurd/spawn! app "panic-task" nil)
            snapshot (absurd/await-task-result app (:task-id panicked) {:timeout 10})]
        (is (= :failed (:state snapshot)))
        (is (= "boom" (get-in snapshot [:failure :message])))
        (is (= "Error" (get-in snapshot [:failure :name]))))
      (is (true? (:running? @worker)))
      (let [ok (absurd/spawn! app "ok-task" nil)]
        (is (= {:state :completed
                :result {:ok true}}
               (absurd/await-task-result app (:task-id ok) {:timeout 10}))))
      (finally
        (absurd/stop-worker! worker)))))

(deftest worker-shutdown-drains-claimed-tasks
  (let [app (pooled-client)
        started (CountDownLatch. 1)
        release (CountDownLatch. 1)
        _ (absurd/register-task! app "slow-task"
                                 (fn [_ _] (.countDown started) (await-latch release 5) "done")
                                 {:max-attempts 1})
        worker (absurd/start-worker! app {:worker-id "shutdown-worker"
                                          :concurrency 1
                                          :poll-interval 25})
        spawned (absurd/spawn! app "slow-task" nil)]
    (is (await-latch started 5))
    (let [stop-result (future (absurd/stop-worker! worker))]
      (Thread/sleep 200)
      (is (not (realized? stop-result)) "stop waits for the in-flight task")
      (.countDown release)
      (is (true? (deref stop-result 5000 ::timeout))))
    (is (= {:state :completed
            :result "done"} (absurd/fetch-task-result app (:task-id spawned))))
    (is (= 1 (count (h/get-runs h/*conn* (:queue app) (:task-id spawned)))))))

(deftest stop-worker-returns-false-when-shutdown-times-out
  (let [app (pooled-client)
        started (CountDownLatch. 1)
        _ (absurd/register-task! app "long-task"
                                 (fn [_ _] (.countDown started) (Thread/sleep 1000) "late")
                                 {:max-attempts 1})
        worker (absurd/start-worker! app {:worker-id "timeout-worker"
                                          :concurrency 1
                                          :poll-interval 25
                                          :shutdown-timeout 0.2})
        spawned (absurd/spawn! app "long-task" nil)]
    (is (await-latch started 5))
    (is (false? (absurd/stop-worker! worker)))
    (testing "the task still completes after the worker gave up waiting"
      (is (= {:state :completed
              :result "late"}
             (absurd/await-task-result app (:task-id spawned) {:timeout 5}))))
    (testing "stopping twice is a no-op"
      (is (false? (absurd/stop-worker! worker))))))

(deftest worker-handle-is-closeable-and-registered-on-the-client
  (let [app (pooled-client)]
    (with-open [worker (absurd/start-worker! app {:worker-id "closeable-worker"
                                                  :poll-interval 25})]
      (is (identical? worker @(:worker app)))
      (is (true? (:running? @worker)))
      (is (str/starts-with? (:worker-id @worker) "closeable-worker")))
    (is (nil? @(:worker app)))
    (testing "close! on the client stops a running worker"
      (let [worker (absurd/start-worker! app {:worker-id "closed-by-client"
                                              :poll-interval 25})]
        (absurd/close! app)
        (is (false? (:running? @worker)))))))

(deftest lease-watchdog-warns-and-calls-the-hook
  (let [calls (atom [])
        app (-> (pooled-client {:hooks {:on-lease-timeout (fn [ctx seconds] (swap! calls conj [(:task-id ctx) seconds]))}})
                (absurd/register-task! "overrun" (fn [_ _] (Thread/sleep 1500) "slow") {:max-attempts 1})
                (absurd/register-task! "quick" (fn [_ _] "fast") {:max-attempts 1}))
        worker (absurd/start-worker! app {:worker-id "watchdog-worker"
                                          :claim-timeout 1
                                          :poll-interval 25})]
    (try
      (testing "a task overrunning its claim timeout triggers the hook once"
        (let [spawned (absurd/spawn! app "overrun" nil)]
          (absurd/await-task-result app (:task-id spawned) {:timeout 5})
          (is (wait-until #(= 1 (count @calls)) 2))
          (is (= [(:task-id spawned) 1] (first @calls)))))
      (testing "a task that finishes in time never triggers the hook"
        (reset! calls [])
        (let [spawned (absurd/spawn! app "quick" nil)]
          (absurd/await-task-result app (:task-id spawned) {:timeout 5})
          (Thread/sleep 1500)
          (is (= [] @calls))))
      (finally
        (absurd/stop-worker! worker)))))

(deftest heartbeat-reschedules-the-watchdog
  (let [calls (atom 0)
        app (-> (pooled-client {:hooks {:on-lease-timeout (fn [_ _] (swap! calls inc))}})
                (absurd/register-task! "heartbeater"
                                       (fn [_ ctx]
                                         (dotimes [_ 3]
                                           (Thread/sleep 500)
                                           (absurd/heartbeat! ctx 2))
                                         "kept-alive")
                                       {:max-attempts 1}))
        worker (absurd/start-worker! app {:worker-id "heartbeat-worker"
                                          :claim-timeout 1
                                          :poll-interval 25})]
    (try
      (let [spawned (absurd/spawn! app "heartbeater" nil)]
        (is (= {:state :completed
                :result "kept-alive"}
               (absurd/await-task-result app (:task-id spawned) {:timeout 10})))
        (Thread/sleep 300)
        (is (= 0 @calls)))
      (finally
        (absurd/stop-worker! worker)))))

(deftest only-one-worker-per-client
  (let [app (pooled-client)
        worker (absurd/start-worker! app {:worker-id "first"
                                          :poll-interval 25})]
    (try
      (is (= :absurd/worker-already-running
             (h/ex-type #(absurd/start-worker! app {:worker-id "second"
                                                    :poll-interval 25}))))
      (is (identical? worker @(:worker app)))
      (finally
        (absurd/stop-worker! worker)))
    (testing "after stopping, a new worker can start"
      (with-open [again (absurd/start-worker! app {:worker-id "third"
                                                   :poll-interval 25})]
        (is (true? (:running? @again)))))))

(deftest stop-worker-returns-promptly-from-a-long-poll-sleep
  (let [app (pooled-client)
        worker (absurd/start-worker! app {:worker-id "sleepy"
                                          :poll-interval 5000
                                          :shutdown-timeout 1})
        started (System/nanoTime)]
    (Thread/sleep 200)
    (is (true? (absurd/stop-worker! worker)))
    (is (< (/ (- (System/nanoTime) started) 1e6) 1500))))
