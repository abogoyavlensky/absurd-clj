(ns absurd.core
  "Clojure SDK for Absurd: client, task registry, spawning, control plane,
  task context operations and workers.

  A client is an open map built by `client`. Handlers are plain functions
  `(fn [params ctx] ...)`. Every context operation takes the context first."
  (:require [absurd.sql :as sql]
            [clojure.string :as str]
            [next.jdbc :as jdbc])
  (:import [java.net URI URLEncoder]
           [java.nio.charset StandardCharsets]
           [java.sql Connection]
           [java.time Instant]
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
  and doubles up to 1 s. `timeout` is in seconds; nil waits forever."
  [fetch task-id timeout on-before-sleep]
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
                           delay-ms)]
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
     (poll-task-result #(fetch-task-result x task-id {:queue queue}) task-id timeout nil)
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

;; Placeholders completed in later tasks

(defn- context-emit-event!
  [_ctx _event-name _payload]
  (fail "task context operations are not available yet" :absurd/not-implemented {}))

(defn- context-await-task-result
  [_ctx _task-id _opts]
  (fail "task context operations are not available yet" :absurd/not-implemented {}))

(defn stop-worker!
  [_worker]
  nil)
