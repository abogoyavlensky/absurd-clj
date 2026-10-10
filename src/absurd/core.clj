(ns absurd.core
  "Clojure SDK for Absurd: client, task registry, spawning, control plane,
  task context operations and workers.

  A client is an open map built by `client`. Handlers are plain functions
  `(fn [params ctx] ...)`. Every context operation takes the context first."
  (:require [absurd.json :as json]
            [absurd.sql :as sql]
            [clojure.stacktrace :as stacktrace]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc])
  (:import [clojure.lang IDeref]
           [java.io Closeable]
           [java.lang ProcessHandle]
           [java.net InetAddress URI URLEncoder]
           [java.nio.charset StandardCharsets]
           [java.sql Connection]
           [java.time Instant OffsetDateTime]
           [java.util Date]
           [java.util.concurrent CountDownLatch ExecutorService Executors ScheduledExecutorService
            ScheduledFuture Semaphore ThreadFactory TimeUnit]
           [java.util.concurrent.locks ReentrantLock]
           [javax.sql DataSource]))

(def ^:private max-queue-name-bytes 57)

(defn- fail
  [message signal data]
  (throw (ex-info message (assoc data :type signal))))

(defn- enum-str
  [v]
  (some-> v name))

;; Connection resolution

(defn resolve-database-url
  "The connection string to use: `url` when given, else `ABSURD_DATABASE_URL`,
  else `PGDATABASE`, else `postgresql://localhost/absurd`."
  ([] (resolve-database-url (System/getenv) nil))
  ([env url]
   (or (not-empty url)
       (not-empty (get env "ABSURD_DATABASE_URL"))
       (not-empty (get env "PGDATABASE"))
       "postgresql://localhost/absurd")))

(defn- query-param
  "A `key=value` pair with the value URL-encoded the way pgjdbc decodes it."
  [k ^String v]
  (str k "=" (URLEncoder/encode v StandardCharsets/UTF_8)))

(defn ->jdbc-url
  "Convert a `postgresql://` or `postgres://` URL, a bare database name, or a
  `jdbc:` URL into a JDBC URL for the Postgres driver. Credentials in the
  URL authority become `user` and `password` query parameters."
  ^String [^String s]
  (cond
    (str/starts-with? s "jdbc:")
    s

    (re-find #"^(postgresql|postgres)://" s)
    (let [uri (URI. s)
          host (or (.getHost uri) "localhost")
          port (.getPort uri)
          [user password] (some-> (.getUserInfo uri) (str/split #":" 2))
          params (cond-> []
                   user (conj (query-param "user" user))
                   password (conj (query-param "password" password))
                   (.getRawQuery uri) (conj (.getRawQuery uri)))]
      (str "jdbc:postgresql://" host
           (when (pos? port) (str ":" port))
           (.getRawPath uri)
           (when (seq params) (str "?" (str/join "&" params)))))

    :else
    (str "jdbc:postgresql://localhost/" s)))

(defn- ->connectable
  [db]
  (cond
    (nil? db) (jdbc/get-datasource (->jdbc-url (resolve-database-url)))
    (string? db) (jdbc/get-datasource (->jdbc-url db))
    (instance? Connection db) db
    (instance? DataSource db) db
    :else (fail "Unsupported :db value; expected a DataSource, Connection, URL string or nil"
                :absurd/invalid-db {:db db})))

;; Validation

(defn validate-queue-name
  "Return `queue` or throw `:absurd/invalid-queue-name`."
  [queue]
  (when (or (nil? queue) (= "" queue))
    (fail "Queue name must be provided" :absurd/invalid-queue-name {:queue queue}))
  (when (> (count (.getBytes ^String queue StandardCharsets/UTF_8)) max-queue-name-bytes)
    (fail (str "Queue name '" queue "' is too long (max " max-queue-name-bytes " bytes).")
          :absurd/invalid-queue-name {:queue queue}))
  queue)

(defn- validate-event-name
  [event-name]
  (when (or (nil? event-name) (= "" event-name))
    (fail "event-name must be a non-empty string" :absurd/invalid-event-name {:event-name event-name}))
  event-name)

(defn- queue-of
  "The validated queue to use: `queue` when given, else the client's."
  [app queue]
  (validate-queue-name (or queue (:queue app))))

(defn- client?
  [x]
  (contains? x :registry))

;; Client

(defn client
  "Create a client. Options:

  | key            | default              | meaning |
  |----------------|----------------------|---------|
  | `:db`          | env-resolved URL     | DataSource, Connection, URL string or nil |
  | `:queue`       | `\"default\"`        | default queue |
  | `:max-attempts`| `5`                  | default retry limit |
  | `:hooks`       | `{}`                 | `:before-spawn`, `:wrap-task-execution`, `:on-lease-timeout` |
  | `:now-fn`      | `Instant/now`        | clock used for sleeps |"
  ([] (client {}))
  ([{:keys [db queue max-attempts hooks now-fn]
     :or {queue "default"
          max-attempts 5
          hooks {}
          now-fn #(Instant/now)}}]
   (validate-queue-name queue)
   {:db (->connectable db)
    :owned-db? (or (nil? db) (string? db))
    :queue queue
    :max-attempts max-attempts
    :hooks hooks
    :now-fn now-fn
    :registry (atom {})
    :worker (atom nil)}))

(declare stop-worker!)

(defn close!
  "Stop a running worker and release a connection the client opened itself."
  [app]
  (when-let [worker @(:worker app)]
    (stop-worker! worker))
  (when (and (:owned-db? app) (instance? Connection (:db app)))
    (.close ^Connection (:db app)))
  nil)

(defn register-task!
  "Register `handler` as task `task-name`. Returns the client.
  Options: `:queue`, `:max-attempts`, `:cancellation`."
  ([app task-name handler] (register-task! app task-name handler {}))
  ([app task-name handler {:keys [queue max-attempts cancellation]}]
   (let [queue (validate-queue-name (or queue (:queue app)))]
     (swap! (:registry app) assoc task-name {:name task-name
                                             :queue queue
                                             :max-attempts max-attempts
                                             :cancellation cancellation
                                             :handler handler})
     app)))

;; Option payloads (explicit keys only; user data passes through untouched)

(defn- retry-strategy->json
  [{:keys [kind base-seconds factor max-seconds]}]
  (cond-> {"kind" (enum-str kind)}
    (some? base-seconds) (assoc "base_seconds" base-seconds)
    (some? factor) (assoc "factor" factor)
    (some? max-seconds) (assoc "max_seconds" max-seconds)))

(defn- cancellation->json
  [{:keys [max-duration max-delay]}]
  (not-empty
    (cond-> {}
      (some? max-duration) (assoc "max_duration" max-duration)
      (some? max-delay) (assoc "max_delay" max-delay))))

(defn- spawn-options->json
  [{:keys [headers max-attempts retry-strategy cancellation idempotency-key]}]
  (cond-> {}
    (some? headers) (assoc "headers" headers)
    (some? max-attempts) (assoc "max_attempts" max-attempts)
    (seq retry-strategy) (assoc "retry_strategy" (retry-strategy->json retry-strategy))
    (cancellation->json cancellation) (assoc "cancellation" (cancellation->json cancellation))
    (some? idempotency-key) (assoc "idempotency_key" idempotency-key)))

(defn- queue-policy->json
  [{:keys [partition-lookahead partition-lookback cleanup-ttl cleanup-limit detach-mode detach-min-age]}]
  (cond-> {}
    (some? partition-lookahead) (assoc "partition_lookahead" partition-lookahead)
    (some? partition-lookback) (assoc "partition_lookback" partition-lookback)
    (some? cleanup-ttl) (assoc "cleanup_ttl" cleanup-ttl)
    (some? cleanup-limit) (assoc "cleanup_limit" cleanup-limit)
    (some? detach-mode) (assoc "detach_mode" (enum-str detach-mode))
    (some? detach-min-age) (assoc "detach_min_age" detach-min-age)))

(defn- retry-options->json
  [{:keys [max-attempts spawn-new?]}]
  (cond-> {}
    (some? max-attempts) (assoc "max_attempts" max-attempts)
    spawn-new? (assoc "spawn_new" true)))

;; Spawning

(defn- prepare-spawn
  "Resolve the target queue and the JSON spawn options for `task-name`."
  [app task-name {:keys [queue max-attempts cancellation]
                  :as opts}]
  (let [registration (get @(:registry app) task-name)
        target-queue (cond
                       registration
                       (let [registered-queue (:queue registration)]
                         (when (and queue (not= queue registered-queue))
                           (fail (str "Task \"" task-name "\" is registered for queue \""
                                      registered-queue "\" but spawn requested queue \"" queue "\".")
                                 :absurd/queue-mismatch {:task-name task-name
                                                         :queue queue
                                                         :registered-queue registered-queue}))
                         registered-queue)

                       (nil? queue)
                       (fail (str "Task \"" task-name "\" is not registered. "
                                  "Provide :queue when spawning unregistered tasks.")
                             :absurd/unregistered-task {:task-name task-name})

                       :else queue)
        max-attempts (or max-attempts (:max-attempts registration) (:max-attempts app))
        cancellation (or cancellation (:cancellation registration))]
    [(validate-queue-name target-queue)
     (spawn-options->json (assoc opts :max-attempts max-attempts :cancellation cancellation))]))

(defn spawn!
  "Enqueue `task-name` with `params`. Options: `:queue`, `:max-attempts`,
  `:retry-strategy`, `:headers`, `:cancellation`, `:idempotency-key`.
  Returns `{:task-id :run-id :attempt :created}`."
  ([app task-name params] (spawn! app task-name params {}))
  ([app task-name params opts]
   (let [hook (get-in app [:hooks :before-spawn])
         opts (if hook (hook task-name params opts) opts)
         [queue options] (prepare-spawn app task-name opts)]
     (sql/spawn-task! (:db app) queue task-name params options))))

;; Task results

(defn- ->snapshot
  [{:keys [state result failure-reason]}]
  (let [state (keyword state)]
    (case state
      :completed {:state state
                  :result result}
      :failed {:state state
               :failure failure-reason}
      {:state state})))

(defn- terminal-state?
  [state]
  (contains? #{:completed :failed :cancelled} state))

(defn- poll-task-result
  "Poll `fetch` until it returns a terminal snapshot. Delay starts at 50 ms
  and doubles up to 1 s, capped by `max-sleep-ms` when given. `timeout` is
  in seconds; nil waits forever. `on-before-sleep` runs before each pause."
  [fetch task-id timeout on-before-sleep max-sleep-ms]
  (let [timeout-ms (some-> timeout (max 0) (* 1000.0))
        started (System/nanoTime)]
    (loop [delay-ms 50.0]
      (let [snapshot (fetch)]
        (cond
          (nil? snapshot)
          (fail (str "Task \"" task-id "\" not found") :absurd/task-not-found {:task-id task-id})

          (terminal-state? (:state snapshot))
          snapshot

          :else
          (let [sleep-ms (if timeout-ms
                           (let [elapsed-ms (/ (- (System/nanoTime) started) 1e6)
                                 remaining-ms (- timeout-ms elapsed-ms)]
                             (when (<= remaining-ms 0)
                               (fail (str "Timed out waiting for task \"" task-id "\"")
                                     :absurd/timeout {:task-id task-id}))
                             (min delay-ms remaining-ms))
                           delay-ms)
                sleep-ms (if max-sleep-ms (min sleep-ms max-sleep-ms) sleep-ms)]
            (when on-before-sleep
              (on-before-sleep))
            (Thread/sleep (long (max sleep-ms 0)))
            (recur (min (* sleep-ms 2) 1000.0))))))))

(defn fetch-task-result
  "Current task snapshot: `{:state ...}` plus `:result` when completed or
  `:failure` when failed. nil when the task does not exist."
  ([app task-id] (fetch-task-result app task-id {}))
  ([app task-id {:keys [queue]}]
   (some-> (sql/get-task-result (:db app) (queue-of app queue) task-id)
           ->snapshot)))

(declare context-await-task-result)

(defn await-task-result
  "Wait for a task to reach a terminal state. On a client this polls the
  database; inside a task context the wait is checkpointed as a step.
  Options: `:timeout` (seconds), `:queue`, and `:step-name` (context only)."
  ([x task-id] (await-task-result x task-id {}))
  ([x task-id {:keys [queue timeout]
               :as opts}]
   (if (client? x)
     (poll-task-result #(fetch-task-result x task-id {:queue queue}) task-id timeout nil nil)
     (context-await-task-result x task-id opts))))

;; Events, cancellation, retries

(declare context-emit-event!)

(defn emit-event!
  "Emit an event (first emit per name wins). On a client the queue defaults
  to the client's queue and can be overridden with `{:queue ...}`; inside a
  task context the event goes to the task's queue."
  ([x event-name payload] (emit-event! x event-name payload {}))
  ([x event-name payload {:keys [queue]}]
   (validate-event-name event-name)
   (if (client? x)
     (sql/emit-event! (:db x) (queue-of x queue) event-name payload)
     (context-emit-event! x event-name payload))))

(defn cancel-task!
  ([app task-id] (cancel-task! app task-id {}))
  ([app task-id {:keys [queue]}]
   (sql/cancel-task! (:db app) (queue-of app queue) task-id)))

(defn retry-task!
  "Retry a failed task in place, or spawn a fresh task from the same inputs
  with `:spawn-new? true`. Options: `:max-attempts`, `:spawn-new?`, `:queue`.
  Returns `{:task-id :run-id :attempt :created}`."
  ([app task-id] (retry-task! app task-id {}))
  ([app task-id {:keys [queue]
                 :as opts}]
   (sql/retry-task! (:db app) (queue-of app queue) task-id (retry-options->json opts))))

;; Queue management

(defn set-queue-policy!
  "Update maintenance policy fields. Options: `:partition-lookahead`,
  `:partition-lookback`, `:cleanup-ttl`, `:cleanup-limit`, `:detach-mode`,
  `:detach-min-age`. A map with none of them is a no-op."
  [app queue opts]
  (let [policy (queue-policy->json opts)]
    (when (seq policy)
      (sql/set-queue-policy! (:db app) (queue-of app queue) policy))
    nil))

(defn create-queue!
  "Create a queue (defaults to the client's). Options: `:storage-mode`
  (`:unpartitioned` or `:partitioned`) plus the `set-queue-policy!` keys."
  ([app] (create-queue! app nil {}))
  ([app queue] (create-queue! app queue {}))
  ([app queue {:keys [storage-mode]
               :as opts}]
   (let [queue (queue-of app queue)
         mode (enum-str (or storage-mode :unpartitioned))]
     (when-not (contains? #{"unpartitioned" "partitioned"} mode)
       (fail "storage-mode must be :unpartitioned or :partitioned"
             :absurd/invalid-storage-mode {:storage-mode storage-mode}))
     (sql/create-queue! (:db app) queue (when (= mode "partitioned") mode))
     (set-queue-policy! app queue opts))))

(defn get-queue-policy
  "The queue's maintenance policy with interval fields as strings, or nil."
  ([app] (get-queue-policy app nil))
  ([app queue]
   (some-> (sql/get-queue-policy (:db app) (queue-of app queue))
           (update :storage-mode keyword)
           (update :detach-mode keyword))))

(defn drop-queue!
  ([app] (drop-queue! app nil))
  ([app queue]
   (sql/drop-queue! (:db app) (queue-of app queue))))

(defn list-queues
  "All queue names."
  [app]
  (sql/list-queues (:db app)))

;; Claiming

(defn claim-tasks!
  "Claim up to `:batch-size` tasks from the client's queue.
  Options: `:batch-size` (1), `:claim-timeout` seconds (120), `:worker-id`."
  ([app] (claim-tasks! app {}))
  ([app {:keys [batch-size claim-timeout worker-id]
         :or {batch-size 1
              claim-timeout 120
              worker-id "worker"}}]
   (sql/claim-tasks! (:db app) (:queue app) worker-id claim-timeout batch-size)))

;; Task context

(def ^:dynamic *context*
  "The task context of the handler running on this thread, or nil."
  nil)

(defn current-context
  "The current task context when called inside a handler, else nil."
  []
  *context*)

(def ^:private not-found ::not-found)

(defn- ctx-db
  [ctx]
  (:db (::client ctx)))

(defn- decode-headers
  [headers]
  (cond
    (nil? headers) {}
    (map? headers) headers
    :else (fail "invalid task headers: headers payload must be a JSON object"
                :absurd/invalid-headers {:headers headers})))

(declare lease-touch! lease-stop!)

(defn- make-context
  "Build the context for a claimed task, preloading its checkpoints."
  [app claim claim-timeout lease]
  (let [queue (:queue app)
        headers (decode-headers (:headers claim))
        checkpoints (into {}
                          (map (juxt :checkpoint-name :state))
                          (sql/get-task-checkpoint-states (:db app) queue (:task-id claim) (:run-id claim)))]
    {:task-id (:task-id claim)
     :run-id (:run-id claim)
     :task-name (:task-name claim)
     :attempt (:attempt claim)
     :queue queue
     :headers headers
     ::client app
     ::claim-timeout claim-timeout
     ::wake-event (atom {:event (:wake-event claim)
                         :payload (:event-payload claim)})
     ::checkpoints (atom checkpoints)
     ::step-counter (atom {})
     ::lease lease}))

(defn- checkpoint-name!
  "The concrete checkpoint name for `step-name` in this run: `name`, then
  `name#2`, `name#3` for repeated uses."
  [ctx step-name]
  (let [n (get (swap! (::step-counter ctx) update step-name (fnil inc 0)) step-name)]
    (if (= 1 n) step-name (str step-name "#" n))))

(defn- lookup-checkpoint
  [ctx checkpoint-name]
  (let [cache @(::checkpoints ctx)]
    (if (contains? cache checkpoint-name)
      (get cache checkpoint-name)
      (if-let [row (sql/get-task-checkpoint-state (ctx-db ctx) (:queue ctx) (:task-id ctx) checkpoint-name)]
        (do (swap! (::checkpoints ctx) assoc checkpoint-name (:state row))
            (:state row))
        not-found))))

(defn- persist-checkpoint!
  [ctx checkpoint-name value]
  (sql/set-task-checkpoint-state! (ctx-db ctx) (:queue ctx) (:task-id ctx) checkpoint-name
                                  value (:run-id ctx) (::claim-timeout ctx))
  (swap! (::checkpoints ctx) assoc checkpoint-name value)
  (lease-touch! ctx (::claim-timeout ctx))
  nil)

(defn begin-step
  "Start a step. Returns a handle `{:name :checkpoint-name :done? :state}`;
  when `:done?` is true the step already ran and `:state` holds its value."
  [ctx step-name]
  (let [checkpoint-name (checkpoint-name! ctx step-name)
        state (lookup-checkpoint ctx checkpoint-name)]
    (if (identical? state not-found)
      {:name step-name
       :checkpoint-name checkpoint-name
       :done? false}
      {:name step-name
       :checkpoint-name checkpoint-name
       :done? true
       :state state})))

(defn complete-step
  "Persist `value` for a step started with `begin-step`. Returns the value as
  it will be seen on replay (after a JSON round trip)."
  [ctx handle value]
  (if (:done? handle)
    (:state handle)
    (let [value (json/round-trip value)]
      (persist-checkpoint! ctx (:checkpoint-name handle) value)
      value)))

(defn step
  "Run `f` once per task under `step-name` and checkpoint its result.
  Replays return the stored value without calling `f`."
  [ctx step-name f]
  (let [handle (begin-step ctx step-name)]
    (if (:done? handle)
      (:state handle)
      (complete-step ctx handle (f)))))

(defn heartbeat!
  "Extend the current run's lease by `seconds` (default: the claim timeout)."
  ([ctx] (heartbeat! ctx (::claim-timeout ctx)))
  ([ctx seconds]
   (sql/extend-claim! (ctx-db ctx) (:queue ctx) (:run-id ctx) seconds)
   (lease-touch! ctx seconds)
   nil))

(defn- context-emit-event!
  [ctx event-name payload]
  (sql/emit-event! (ctx-db ctx) (:queue ctx) event-name payload))

;; Sleeping

(defn- suspend!
  []
  (throw (ex-info "task suspended" {:type :absurd/suspend})))

(defn- now
  ^Instant [ctx]
  ((:now-fn (::client ctx))))

(defn- parse-instant
  ^Instant [^String s]
  (.toInstant (OffsetDateTime/parse s)))

(defn- ->instant
  ^Instant [wake-at]
  (cond
    (instance? Instant wake-at) wake-at
    (instance? Date wake-at) (.toInstant ^Date wake-at)
    (number? wake-at) (Instant/ofEpochMilli (long (* 1000 wake-at)))
    (string? wake-at) (parse-instant wake-at)
    :else (fail "wake-at must be an Instant, Date, epoch seconds or ISO-8601 string"
                :absurd/invalid-wake-at {:wake-at wake-at})))

(defn sleep-until
  "Suspend the task until `wake-at` (an Instant, Date, epoch seconds or ISO
  string). The wake time is checkpointed under `step-name`, so a replay keeps
  the original wake time. Returns nil once the wake time has passed."
  [ctx step-name wake-at]
  (let [wake-at (->instant wake-at)
        checkpoint-name (checkpoint-name! ctx step-name)
        state (lookup-checkpoint ctx checkpoint-name)
        actual-wake-at (cond
                         (identical? state not-found)
                         (do (persist-checkpoint! ctx checkpoint-name (str wake-at))
                             wake-at)

                         (string? state) (parse-instant state)
                         :else wake-at)
        remaining-seconds (/ (- (.toEpochMilli actual-wake-at) (.toEpochMilli (now ctx))) 1000.0)]
    (when (pos? remaining-seconds)
      (sql/schedule-run-after! (ctx-db ctx) (:queue ctx) (:run-id ctx) remaining-seconds)
      (suspend!))
    nil))

(defn sleep-for
  "Suspend the task for `seconds`, checkpointed under `step-name`."
  [ctx step-name seconds]
  (sleep-until ctx step-name (.plusMillis (now ctx) (long (* 1000 seconds)))))

;; Events

(defn await-event
  "Suspend until `event-name` is emitted and return its payload. Options:
  `:timeout` seconds (throws `:absurd/timeout` once it expires; negative
  values behave like 0) and `:step-name` (default `$awaitEvent:<event-name>`)."
  ([ctx event-name] (await-event ctx event-name {}))
  ([ctx event-name {:keys [timeout step-name]}]
   (validate-event-name event-name)
   (let [checkpoint-name (checkpoint-name! ctx (or step-name (str "$awaitEvent:" event-name)))
         cached (lookup-checkpoint ctx checkpoint-name)]
     (if-not (identical? cached not-found)
       cached
       (let [{:keys [event payload]} @(::wake-event ctx)]
         (if (and (= event event-name) (nil? payload))
           (do (reset! (::wake-event ctx) {:event nil
                                           :payload nil})
               (fail (str "Timed out waiting for event \"" event-name "\"")
                     :absurd/timeout {:event-name event-name}))
           (let [{:keys [should-suspend payload]}
                 (sql/await-event (ctx-db ctx) (:queue ctx) (:task-id ctx) (:run-id ctx)
                                  checkpoint-name event-name (some-> timeout (max 0)))]
             (when should-suspend
               (suspend!))
             (swap! (::checkpoints ctx) assoc checkpoint-name payload)
             (swap! (::wake-event ctx) assoc :payload nil)
             payload)))))))

;; Durable task-result waits

(defn- snapshot->json
  [{:keys [state result failure]}]
  (case state
    :completed {:state (name state)
                :result result}
    :failed {:state (name state)
             :failure failure}
    {:state (name state)}))

(defn- snapshot<-json
  [{:keys [state result failure]}]
  (let [state (keyword state)]
    (case state
      :completed {:state state
                  :result result}
      :failed {:state state
               :failure failure}
      {:state state})))

(defn- context-await-task-result
  [ctx task-id {:keys [queue timeout step-name]}]
  (let [queue (validate-queue-name (or queue (:queue ctx)))]
    (when (= queue (:queue ctx))
      (fail (str "await-task-result cannot wait on tasks in the same queue because this can "
                 "deadlock workers. Spawn the child in a different queue and pass :queue.")
            :absurd/same-queue-wait {:task-id task-id
                                     :queue queue}))
    (let [app (::client ctx)
          wait (fn []
                 ;; Refresh the lease first: earlier handler work may have used
                 ;; most of the claim, and heartbeats are paced from here.
                 (heartbeat! ctx)
                 (let [interval-ms (max 500.0 (/ (* (::claim-timeout ctx) 1000.0) 2))
                       next-heartbeat-at (atom (+ (System/currentTimeMillis) interval-ms))
                       maybe-heartbeat! (fn []
                                          (when (>= (System/currentTimeMillis) @next-heartbeat-at)
                                            (heartbeat! ctx)
                                            (reset! next-heartbeat-at (+ (System/currentTimeMillis) interval-ms))))]
                   (snapshot->json
                     (poll-task-result #(fetch-task-result app task-id {:queue queue})
                                       task-id timeout maybe-heartbeat! (/ interval-ms 2)))))]
      (snapshot<-json (step ctx (or step-name (str "$awaitTaskResult:" task-id)) wait)))))

;; Lease watchdog

(defn- daemon-thread-factory
  ^ThreadFactory [prefix]
  (let [counter (atom 0)]
    (reify ThreadFactory
      (newThread [_ runnable]
        (doto (Thread. ^Runnable runnable (str prefix "-" (swap! counter inc)))
          (.setDaemon true))))))

(defonce ^:private watchdog-scheduler
  (delay (Executors/newSingleThreadScheduledExecutor (daemon-thread-factory "absurd-watchdog"))))

(defmacro ^:private with-lock
  "Run `body` while holding `lock` (a ReentrantLock); `locking` is avoided
  because coverage tooling cannot instrument monitor-enter."
  [lock & body]
  `(let [^ReentrantLock lock# ~lock]
     (.lock lock#)
     (try
       ~@body
       (finally
         (.unlock lock#)))))

(defn- new-lease
  []
  (atom {:epoch 0
         :stopped? false
         :scheduled nil
         :lock (ReentrantLock.)}))

(defn- lease-expired!
  [ctx seconds]
  (log/warnf "task %s (%s) exceeded claim timeout of %ss" (:task-name ctx) (:task-id ctx) seconds)
  (when-let [hook (get-in (::client ctx) [:hooks :on-lease-timeout])]
    (try
      (hook ctx seconds)
      (catch Throwable t
        (log/warn t "on-lease-timeout hook failed")))))

(defn- lease-touch!
  "(Re)schedule the watchdog to fire `seconds` from now. Stale callbacks
  from earlier schedules are ignored through the epoch."
  [ctx seconds]
  (when-let [lease (::lease ctx)]
    (when (pos? seconds)
      (with-lock (:lock @lease)
        (let [{:keys [stopped? ^ScheduledFuture scheduled]} @lease]
          (when-not stopped?
            (some-> scheduled (.cancel false))
            (let [epoch (:epoch (swap! lease update :epoch inc))
                  callback (fn []
                             (let [current @lease]
                               (when (and (not (:stopped? current)) (= epoch (:epoch current)))
                                 (lease-expired! ctx seconds))))
                  scheduled (.schedule ^ScheduledExecutorService @watchdog-scheduler
                                       ^Runnable callback
                                       (long (* 1000 seconds))
                                       TimeUnit/MILLISECONDS)]
              (swap! lease assoc :scheduled scheduled))))))))

(defn- lease-stop!
  [lease]
  (when lease
    (with-lock (:lock @lease)
      (let [{:keys [^ScheduledFuture scheduled]} @lease]
        (some-> scheduled (.cancel false))
        (swap! lease assoc :stopped? true :scheduled nil :epoch (inc (:epoch @lease)))))))

;; Execution

(def ^:private unknown-task-defer-base-seconds 15)
(def ^:private unknown-task-defer-jitter-seconds 15)

(defn- unknown-task-defer-seconds
  "15 s plus a deterministic jitter of up to 15 s derived from the run id."
  [run-id]
  (+ unknown-task-defer-base-seconds
     (mod (reduce + (map int run-id)) (inc unknown-task-defer-jitter-seconds))))

(defn- signal-type
  [^Throwable t]
  (when (instance? clojure.lang.ExceptionInfo t)
    (:type (ex-data t))))

(defn- terminal-signal?
  [t]
  (contains? #{:absurd/cancelled :absurd/failed-run} (signal-type t)))

(defn- failure-payload
  "The JSON failure payload stored by `fail_run`."
  [^Throwable t]
  {:name (.getSimpleName (class t))
   :message (or (.getMessage t) "")
   :traceback (with-out-str (stacktrace/print-cause-trace t))})

(defn- fail-run!
  "Record a failure for the run, ignoring cancelled/already-failed signals."
  [app run-id ^Throwable t]
  (try
    (sql/fail-run! (:db app) (:queue app) run-id (failure-payload t))
    (catch clojure.lang.ExceptionInfo e
      (when-not (terminal-signal? e)
        (throw e)))))

(defn- defer-unknown-task!
  [app claim]
  (let [run-id (:run-id claim)]
    (try
      (let [defer-seconds (unknown-task-defer-seconds run-id)]
        (sql/schedule-run-after! (:db app) (:queue app) run-id defer-seconds)
        (log/warnf "claimed unknown task %s (%s); deferred run %s by %ds"
                   (:task-name claim) (:task-id claim) run-id defer-seconds))
      (catch Throwable t
        (log/warnf t "failed to defer unknown task %s (%s)" (:task-name claim) (:task-id claim))
        (fail-run! app run-id t)))))

(defn- run-handler!
  [app registration claim claim-timeout lease]
  (let [ctx (make-context app claim claim-timeout lease)
        execute #((:handler registration) (:params claim) ctx)
        wrap (get-in app [:hooks :wrap-task-execution])]
    (lease-touch! ctx claim-timeout)
    (binding [*context* ctx]
      (if wrap
        (wrap ctx execute)
        (execute)))))

(defn- execute-task!
  "Run one claimed task to completion, suspension or failure."
  [app claim claim-timeout lease]
  (let [registration (get @(:registry app) (:task-name claim))
        run-id (:run-id claim)]
    (cond
      (nil? registration)
      (defer-unknown-task! app claim)

      (not= (:queue registration) (:queue app))
      (do (log/warnf "misconfigured task %s (queue mismatch)" (:task-name claim))
          (fail-run! app run-id (ex-info "Misconfigured task (queue mismatch)"
                                         {:type :absurd/queue-mismatch
                                          :task-name (:task-name claim)
                                          :registered-queue (:queue registration)
                                          :queue (:queue app)})))

      :else
      (try
        (let [result (run-handler! app registration claim claim-timeout lease)]
          (sql/complete-run! (:db app) (:queue app) run-id (json/round-trip result)))
        (catch Throwable t
          (case (signal-type t)
            (:absurd/suspend :absurd/cancelled :absurd/failed-run) nil
            (do (log/warnf t "task %s (%s) failed" (:task-name claim) (:task-id claim))
                (fail-run! app run-id t))))
        (finally
          (lease-stop! lease))))
    nil))

(defn work-batch!
  "Claim up to `:batch-size` tasks and run them sequentially on this thread.
  Options: `:worker-id` (\"worker\"), `:claim-timeout` seconds (120), `:batch-size` (1)."
  ([app] (work-batch! app {}))
  ([app {:keys [worker-id claim-timeout batch-size]
         :or {worker-id "worker"
              claim-timeout 120
              batch-size 1}}]
   (doseq [claim (claim-tasks! app {:worker-id worker-id
                                    :claim-timeout claim-timeout
                                    :batch-size batch-size})]
     (execute-task! app claim claim-timeout nil))
   nil))

;; Worker

(defprotocol ^:private WorkerControl
  (-stop! [worker]))

(defn- default-worker-id
  []
  (str (.getHostName (InetAddress/getLocalHost)) ":" (.pid (ProcessHandle/current))))

(defn- pause
  "Wait up to `ms` milliseconds, returning early once `stop-latch` is released."
  [^CountDownLatch stop-latch ms]
  (.await stop-latch (long ms) TimeUnit/MILLISECONDS))

(defn- run-claimed!
  [app claim claim-timeout ^Semaphore capacity]
  (try
    (execute-task! app claim claim-timeout (new-lease))
    (catch Throwable t
      (log/error t "unexpected error while executing task" (:task-name claim)))
    (finally
      (.release capacity))))

(defn- worker-loop
  [app state ^CountDownLatch stop-latch ^Semaphore capacity ^ExecutorService executor]
  (let [{:keys [worker-id claim-timeout batch-size poll-interval]} @state]
    (while (:running? @state)
      (let [available (.availablePermits capacity)]
        (if (zero? available)
          (pause stop-latch poll-interval)
          (let [claims (try
                         (claim-tasks! app {:batch-size (min batch-size available)
                                            :claim-timeout claim-timeout
                                            :worker-id worker-id})
                         (catch Throwable t
                           (log/warn t "worker failed to claim tasks")
                           ::error))]
            (if (or (= ::error claims) (empty? claims))
              (pause stop-latch poll-interval)
              ;; Claimed tasks are always dispatched, even when a stop is in
              ;; progress, so leased work is never stranded.
              (doseq [claim claims]
                (.acquireUninterruptibly capacity)
                (.execute executor ^Runnable #(run-claimed! app claim claim-timeout capacity))))))))))

(defn stop-worker!
  "Stop claiming, wait for in-flight tasks (bounded by `:shutdown-timeout`)
  and shut the pool down. Returns true when every task finished, false when
  the timeout expired with tasks still running. Calling it again is a no-op
  that returns the first result."
  [worker]
  (-stop! worker))

(defn start-worker!
  "Start a polling worker on a background thread and return its handle.
  The handle is `Closeable` (close = `stop-worker!`) and `deref`-able for a
  status map (`:worker-id`, `:running?`, `:concurrency`, `:queue`).

  A client runs at most one worker at a time; starting a second one throws
  `:absurd/worker-already-running`.

  Options: `:worker-id` (hostname:pid), `:claim-timeout` seconds (120),
  `:concurrency` (1), `:batch-size` (= concurrency), `:poll-interval` ms (250),
  `:shutdown-timeout` seconds (60)."
  ([app] (start-worker! app {}))
  ([app {:keys [worker-id claim-timeout concurrency batch-size poll-interval shutdown-timeout]
         :or {claim-timeout 120
              concurrency 1
              poll-interval 250
              shutdown-timeout 60}}]
   (let [worker-id (or worker-id (default-worker-id))
         concurrency (max 1 (long concurrency))
         capacity (Semaphore. concurrency)
         executor (Executors/newFixedThreadPool concurrency (daemon-thread-factory "absurd-task"))
         state (atom {:worker-id worker-id
                      :queue (:queue app)
                      :concurrency concurrency
                      :running? true
                      :claim-timeout claim-timeout
                      :batch-size (or batch-size concurrency)
                      :poll-interval poll-interval})
         handle-ref (atom nil)
         stop-lock (ReentrantLock.)
         stop-latch (CountDownLatch. 1)
         thread (Thread. ^Runnable #(worker-loop app state stop-latch capacity executor)
                         (str "absurd-worker-" worker-id))
         stop! (fn []
                 (with-lock stop-lock
                   (if (contains? @state :drained?)
                     (:drained? @state)
                     (let [deadline (+ (System/nanoTime) (long (* 1e9 shutdown-timeout)))
                           remaining-ms #(max 0 (quot (- deadline (System/nanoTime)) 1000000))]
                       (swap! state assoc :running? false)
                       ;; Wake the loop from its poll wait; a claim in flight finishes
                       ;; and dispatches first. join(0) would block forever, so skip it
                       ;; once the budget is gone.
                       (.countDown stop-latch)
                       (let [budget (remaining-ms)]
                         (when (pos? budget)
                           (.join thread (long budget))))
                       (.shutdown executor)
                       (let [drained? (and (not (.isAlive thread))
                                           (.awaitTermination executor (long (remaining-ms))
                                                              TimeUnit/MILLISECONDS))]
                         (when-not drained?
                           (log/warnf "worker %s stopped with %d task(s) still running"
                                      worker-id (- concurrency (.availablePermits capacity))))
                         (swap! state assoc :drained? drained?)
                         (compare-and-set! (:worker app) @handle-ref nil)
                         drained?)))))
         handle (reify
                  WorkerControl
                  (-stop! [_] (stop!))
                  IDeref
                  (deref [_] @state)
                  Closeable
                  (close [_] (stop!) nil))]
     (reset! handle-ref handle)
     (when-not (compare-and-set! (:worker app) nil handle)
       (.shutdown executor)
       (fail "a worker is already running on this client; stop it first"
             :absurd/worker-already-running {:worker-id (:worker-id @@(:worker app))}))
     (.setDaemon thread true)
     (.start thread)
     handle)))
