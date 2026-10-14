(ns absurd.sqlite.engine
  "The Absurd stored procedures ported to Clojure over SQLite.

  Every procedure is a function over a transaction-scoped connection:
  `(f conn now queue ...)`, where `now` is the engine clock as epoch
  milliseconds, read once per operation the way each PL/pgSQL function reads
  `absurd.current_time()` into `v_now`. Run them inside `with-tx`.

  Values are stored the way the design describes: timestamps as epoch
  milliseconds (`infinity` for Postgres' `'infinity'`), JSON as text, UUIDs as
  text. Procedures take and return the same shapes as `absurd.sql`.

  Errors mirror the PL/pgSQL `raise exception` calls: every one is a
  `java.sql.SQLException` whose message is the Postgres text and whose SQLState
  is AB001, AB002, AB003 or P0001."
  (:require [absurd.json :as json]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.nio.charset StandardCharsets]
           [java.security SecureRandom]
           [java.sql Connection SQLException Statement]
           [java.time Instant]
           [java.util.concurrent.atomic AtomicLong]
           [java.util.concurrent.locks ReentrantLock]
           [javax.sql DataSource]))

;; Errors

(defn raise!
  "Throw an `SQLException` with `message` and `sqlstate`. `sqlstate` defaults
  to P0001, the code of a bare `raise exception`."
  ([message] (raise! "P0001" message))
  ([sqlstate message]
   (throw (SQLException. ^String message ^String sqlstate))))

;; Time

(def infinity
  "The stored value of Postgres' `'infinity'` timestamp."
  Long/MAX_VALUE)

(defn instant->ms
  ^long [^Instant instant]
  (.toEpochMilli instant))

(defn ms->instant
  ^Instant [ms]
  (Instant/ofEpochMilli ms))

;; JSON

(defn encode-json
  "JSON text of `v`. nil is the JSON text `null`; pass nil straight to a
  statement for SQL NULL."
  ^String [v]
  (json/write-str v))

(defn decode-json
  "Decode a JSON column. SQL NULL and the JSON text `null` both give nil."
  [s]
  (some-> s json/read-str))

;; UUIDs

(def ^:private ^SecureRandom secure-random (SecureRandom.))

(def ^:private ^AtomicLong last-uuid-ms (AtomicLong. 0))

(defn uuidv7
  "A UUIDv7 string: 48-bit unix milliseconds, version 7, variant 10 and random
  bits, like `absurd.portable_uuidv7()`. Ids stay strictly increasing within
  the process: when `ms` does not advance past the previous id, the timestamp
  moves one millisecond past it, so `order by run_id` is creation order."
  [ms]
  (let [ts (.updateAndGet last-uuid-ms (reify java.util.function.LongUnaryOperator
                                         (applyAsLong [_ prev] (max (long ms) (inc prev)))))
        rnd (byte-array 10)
        _ (.nextBytes secure-random rnd)
        hi (bit-or (bit-shift-left ts 16)
                   (bit-shift-left 7 12)
                   (bit-and (bit-or (bit-shift-left (bit-and (aget rnd 0) 0xff) 8)
                                    (bit-and (aget rnd 1) 0xff))
                            0x0fff))
        lo (reduce (fn [acc i] (bit-or (bit-shift-left acc 8) (bit-and (aget rnd i) 0xff)))
                   0
                   (range 2 10))
        lo (bit-or (bit-and lo 0x3fffffffffffffff) Long/MIN_VALUE)]
    (str (java.util.UUID. hi lo))))

;; Retry strategy

(def ^:private retry-limit 86400.0)

(defn- json-text
  "The text of a JSON value, like Postgres' `->>`."
  [v]
  (cond
    (nil? v) nil
    (string? v) v
    :else (json/write-str v)))

(defn- number-text->double
  "Parse text as Postgres' `::double precision` would. Unparseable text and
  finite-looking text that overflows raise AB003."
  [^String text]
  (let [t (.trim text)]
    (cond
      (re-matches #"(?i)[-+]?(nan|inf|infinity)" t)
      (Double/parseDouble (-> t (.replaceAll "(?i)infinity|inf" "Infinity") (.replaceAll "(?i)nan" "NaN")))

      (re-matches #"[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?" t)
      (let [d (Double/parseDouble t)
            nonzero-mantissa? (boolean (re-find #"[1-9]" (first (str/split t #"[eE]"))))]
        ;; Postgres rejects overflow and underflow to zero as out of range.
        (if (or (Double/isInfinite d) (and (zero? d) nonzero-mantissa?))
          (raise! "AB003" "retry_strategy contains an invalid number")
          d))

      :else (raise! "AB003" "retry_strategy contains an invalid number"))))

(defn- strategy-number
  [strategy k default]
  (if-let [text (json-text (get strategy k))]
    (number-text->double text)
    default))

(defn retry-delay-seconds
  "Seconds to wait before attempt `attempt + 1`, as `absurd.retry_delay_seconds`:
  a global maximum of one day, explicit invalid values raise AB003, and
  exponential overflow saturates at the configured cap."
  [strategy attempt]
  (let [exponent (max (dec (or attempt 1)) 0)]
    (cond
      (nil? strategy) 0.0

      (not (map? strategy))
      (raise! "AB003" "retry_strategy must be a JSON object")

      :else
      (let [kind (or (json-text (:kind strategy)) "none")]
        (when-not (contains? #{"none" "fixed" "exponential"} kind)
          (raise! "AB003" (str "Unsupported retry strategy kind \"" kind "\"")))
        (if (= "none" kind)
          0.0
          (let [base (strategy-number strategy :base_seconds (if (= "fixed" kind) 60.0 30.0))]
            (when-not (<= 0.0 base retry-limit)
              (raise! "AB003" "retry_strategy.base_seconds must be between 0 and 86400"))
            (if (= "fixed" kind)
              base
              (let [factor (strategy-number strategy :factor 2.0)
                    max-seconds (strategy-number strategy :max_seconds retry-limit)]
                (when (or (neg? factor) (Double/isNaN factor) (Double/isInfinite factor))
                  (raise! "AB003" "retry_strategy.factor must be a finite non-negative number"))
                (when-not (<= 0.0 max-seconds retry-limit)
                  (raise! "AB003" "retry_strategy.max_seconds must be between 0 and 86400"))
                (if (zero? base)
                  0.0
                  (min (* base (Math/pow factor exponent)) max-seconds))))))))))

;; Statements

(def ^:private row-opts
  {:builder-fn rs/as-unqualified-kebab-maps})

(defn query
  "All rows of a select as kebab-case maps."
  [conn sqlvec]
  (jdbc/execute! conn sqlvec row-opts))

(defn query-one
  "The first row of a select, or nil."
  [conn sqlvec]
  (jdbc/execute-one! conn sqlvec row-opts))

(defn update!
  "Run an insert, update or delete and return the number of changed rows."
  [conn sqlvec]
  (long (:next.jdbc/update-count (jdbc/execute-one! conn sqlvec))))

(defn- run-sql!
  [^Connection conn ^String sql]
  (with-open [^Statement st (.createStatement conn)]
    (.execute st sql)))

;; Transactions

(defn- check-autocommit!
  [^Connection conn]
  (when-not (.getAutoCommit conn)
    (throw (ex-info (str "The SQLite connection must have autocommit on: the engine manages "
                         "its own transactions and cannot nest inside one")
                    {:type :absurd/invalid-db}))))

(defn call-in-tx
  "Run `(f conn)` in a transaction on an autocommit connection. `mode` is
  `:write` (`BEGIN IMMEDIATE`, takes the write lock up front so the
  transaction never fails on a later lock upgrade) or `:read` (`BEGIN`).
  Commits when `f` returns and rolls back on any throwable."
  [^Connection conn mode f]
  (run-sql! conn (if (= :read mode) "BEGIN" "BEGIN IMMEDIATE"))
  (let [result (try
                 (f conn)
                 (catch Throwable t
                   (try
                     (run-sql! conn "ROLLBACK")
                     (catch Throwable rollback-failure
                       (.addSuppressed t rollback-failure)))
                   (throw t)))]
    (try
      (run-sql! conn "COMMIT")
      (catch Throwable t
        (try
          (run-sql! conn "ROLLBACK")
          (catch Throwable rollback-failure
            (.addSuppressed t rollback-failure)))
        (throw t)))
    result))

(defn call-with-connection
  "Run `(f conn)` with exclusive use of the environment's connection.

  `env` is `{:connectable c :lock l}`. `c` is a DataSource (a connection is
  borrowed and closed afterwards) or a Connection (used as is, never closed).
  The in-process `ReentrantLock` serializes operations from threads that share
  the environment, which is what makes one shared Connection safe."
  [{:keys [connectable ^ReentrantLock lock]} f]
  (.lock lock)
  (try
    (if (instance? DataSource connectable)
      (with-open [^Connection conn (.getConnection ^DataSource connectable)]
        (check-autocommit! conn)
        (f conn))
      (do (check-autocommit! connectable)
          (f connectable)))
    (finally
      (.unlock lock))))

(defn run-tx
  "Run `(f conn)` in a transaction on the environment's connection. See
  `call-with-connection` and `call-in-tx`."
  [env mode f]
  (call-with-connection env #(call-in-tx % mode f)))

(defmacro with-tx
  "Run `body` with `conn` bound to the environment's connection inside a write
  transaction (`BEGIN IMMEDIATE`):

      (with-tx [conn env] ...)"
  [[conn env] & body]
  `(run-tx ~env :write (fn [~conn] ~@body)))

(defmacro with-read-tx
  "Like `with-tx`, for operations that only read (a deferred `BEGIN`)."
  [[conn env] & body]
  `(run-tx ~env :read (fn [~conn] ~@body)))

;; Shared helpers for the procedures

(defn- normalize
  "A JSON parameter as the data Postgres' jsonb would hold: keyword keys, JSON
  scalars. Callers pass string-keyed or keyword-keyed maps interchangeably."
  [v]
  (json/round-trip v))

(defn- opt-json
  "The JSON text of `k` in `options`, or SQL NULL (nil) when the key is
  absent, like `p_options->'k'`."
  [options k]
  (when (contains? options k)
    (encode-json (get options k))))

(defn- uuid-text
  "A canonical lower-case UUID string, or raise like a `::uuid` cast."
  [id]
  (try
    (str (java.util.UUID/fromString (str id)))
    (catch IllegalArgumentException _
      (raise! "22P02" (str "invalid input syntax for type uuid: \"" id "\"")))))

(defn- int-text
  "Cast a JSON value to a 32-bit integer like `(v->>'k')::integer`: nil stays
  nil, anything else that is not an integer raises."
  [v]
  (when-let [text (json-text v)]
    (if (re-matches #"\s*[-+]?\d+\s*" text)
      (let [n (BigInteger. (str/trim text))]
        (if (<= Integer/MIN_VALUE n Integer/MAX_VALUE)
          (long n)
          (raise! "22003" (str "value \"" (str/trim text) "\" is out of range for type integer"))))
      (raise! "22P02" (str "invalid input syntax for type integer: \"" text "\"")))))

(defn- validate-queue-name
  [queue]
  (when (or (nil? queue) (= "" queue))
    (raise! "Queue name must be provided"))
  (when (> (alength (.getBytes ^String queue StandardCharsets/UTF_8)) 57)
    (raise! (str "Queue name \"" queue "\" is too long (max 57 bytes).")))
  queue)

(defn- require-queue!
  "Raise when the queue does not exist. Postgres fails with an undefined table
  because every queue has its own tables; SQLite shares them."
  [conn queue]
  (when-not (query-one conn ["select 1 as present from absurd_queues where queue_name = ?" queue])
    (raise! "42P01" (str "Queue \"" queue "\" does not exist"))))

;; Queues

(defn create-queue
  "Create a queue. `storage-mode` nil means unpartitioned. Partitioned queues
  are rejected."
  [conn now queue storage-mode]
  (let [queue (validate-queue-name queue)
        mode (str/lower-case (str/trim (or storage-mode "unpartitioned")))]
    (when-not (contains? #{"unpartitioned" "partitioned"} mode)
      (raise! (str "Unsupported queue storage mode \"" storage-mode "\"")))
    (when (= "partitioned" mode)
      (raise! "Partitioned queues are not supported on SQLite"))
    (update! conn ["insert or ignore into absurd_queues (queue_name, created_at, storage_mode)
                    values (?, ?, ?)" queue now mode])
    nil))

(def ^:private queue-tables
  ["absurd_waits" "absurd_events" "absurd_checkpoints" "absurd_runs" "absurd_tasks" "absurd_queues"])

(defn drop-queue
  "Delete a queue with all its tasks, runs, checkpoints, events and waits.
  A missing queue is a no-op."
  [conn _now queue]
  (doseq [table queue-tables]
    (update! conn [(str "delete from " table " where queue_name = ?") queue]))
  nil)

(defn list-queues
  "Queue names as a vector, ordered by name."
  [conn _now]
  (mapv :queue-name (query conn ["select queue_name from absurd_queues order by queue_name"])))

(defn get-queue-policy
  "The queue policy map (interval fields as the stored text), or nil."
  [conn _now queue]
  (query-one conn ["select queue_name, storage_mode, partition_lookahead, partition_lookback,
                           cleanup_ttl, cleanup_limit, detach_mode, detach_min_age
                      from absurd_queues where queue_name = ?" queue]))

(def ^:private policy-keys
  #{:partition_lookahead :partition_lookback :cleanup_ttl :cleanup_limit
    :detach_mode :detach_min_age :default_partition})

(def ^:private interval-pattern
  #"(?i)^\s*(\d+)\s+(second|minute|hour|day|week)s?\s*$")

(defn- interval-text
  "An interval policy value, validated against a narrow grammar and returned
  as given: `<n> second|minute|hour|day|week[s]`."
  [k v]
  (if (and (string? v) (re-matches interval-pattern v))
    v
    (raise! (str "Invalid interval value \"" (or (json-text v) "null") "\" for " (name k)))))

(defn- enum-text
  "`lower(trim(coalesce(v->>'k', '')))`."
  [v]
  (str/lower-case (str/trim (or (json-text v) ""))))

(defn set-queue-policy!
  "Update queue policy fields from a map with snake_case keys. Intervals are
  stored verbatim. A nil or empty policy is a no-op."
  [conn _now queue policy]
  (let [queue (validate-queue-name queue)
        policy (if (nil? policy) {} (normalize policy))]
    (when-not (map? policy)
      (raise! "Queue policy must be a JSON object"))
    (when-let [unknown (first (sort (remove policy-keys (keys policy))))]
      (raise! (str "Unsupported queue policy key \"" (name unknown) "\"")))
    (let [current (query-one conn ["select * from absurd_queues where queue_name = ?" queue])]
      (when-not current
        (raise! (str "Queue \"" queue "\" does not exist")))
      (let [interval (fn [k column]
                       (if (contains? policy k)
                         (interval-text k (get policy k))
                         (get current column)))
            lookahead (interval :partition_lookahead :partition-lookahead)
            lookback (interval :partition_lookback :partition-lookback)
            ttl (interval :cleanup_ttl :cleanup-ttl)
            limit (if (contains? policy :cleanup_limit)
                    (int-text (get policy :cleanup_limit))
                    (:cleanup-limit current))
            detach-mode (if (contains? policy :detach_mode)
                          (enum-text (get policy :detach_mode))
                          (:detach-mode current))
            min-age (interval :detach_min_age :detach-min-age)
            default-partition (if (contains? policy :default_partition)
                                (enum-text (get policy :default_partition))
                                (:default-partition current))]
        (when (or (nil? limit) (< limit 1))
          (raise! "cleanup_limit must be at least 1"))
        (when-not (contains? #{"none" "empty"} detach-mode)
          (raise! (str "Unsupported detach mode \"" detach-mode "\"")))
        (when-not (contains? #{"enabled" "disabled"} default-partition)
          (raise! (str "Unsupported default_partition mode \"" default-partition "\"")))
        (when (contains? policy :default_partition)
          (raise! "default_partition policy is only supported for partitioned queues"))
        (update! conn ["update absurd_queues
                           set default_partition = ?, partition_lookahead = ?, partition_lookback = ?,
                               cleanup_ttl = ?, cleanup_limit = ?, detach_mode = ?, detach_min_age = ?
                         where queue_name = ?"
                       default-partition lookahead lookback ttl limit detach-mode min-age queue])
        nil))))

;; Tasks

(def ^:private insert-task-sql
  "insert into absurd_tasks (queue_name, task_id, task_name, params, headers, retry_strategy,
                             max_attempts, cancellation, enqueue_at, first_started_at, state,
                             attempts, last_attempt_run, completed_payload, cancelled_at,
                             idempotency_key)
   values (?, ?, ?, ?, ?, ?, ?, ?, ?, null, 'pending', 1, ?, null, null, ?)")

(defn- insert-run!
  "Insert a run in `state` that becomes available at `available-at`."
  [conn now queue run-id task-id attempt state available-at]
  (update! conn ["insert into absurd_runs (queue_name, run_id, task_id, attempt, state, available_at,
                                           wake_event, event_payload, result, failure_reason, created_at)
                  values (?, ?, ?, ?, ?, ?, null, null, null, null, ?)"
                 queue run-id task-id attempt state available-at now]))

(defn spawn-task
  "Create a pending task and its first run. `options` is a map with the keys
  `headers`, `retry_strategy`, `max_attempts`, `cancellation` and
  `idempotency_key`. Returns `{:task-id :run-id :attempt :created}`; an
  existing idempotency key returns the existing task with `:created false`."
  [conn now queue task-name params options]
  (when (or (nil? task-name) (str/blank? task-name))
    (raise! "task_name must be provided"))
  (let [options (let [o (normalize options)] (if (map? o) o {}))
        ;; An explicit JSON null is not an absent key: Postgres validates it
        ;; and raises, so the two must stay distinct.
        _ (when (and (contains? options :retry_strategy) (nil? (:retry_strategy options)))
            (raise! "AB003" "retry_strategy must be a JSON object"))
        _ (retry-delay-seconds (:retry_strategy options) 1)
        max-attempts (when (contains? options :max_attempts)
                       (int-text (:max_attempts options)))
        _ (when (and max-attempts (< max-attempts 1))
            (raise! "max_attempts must be >= 1"))
        idempotency-key (json-text (:idempotency_key options))
        _ (require-queue! conn queue)
        task-id (uuidv7 now)
        run-id (uuidv7 now)
        task-params [queue task-id task-name (encode-json params)
                     (opt-json options :headers) (opt-json options :retry_strategy)
                     max-attempts (opt-json options :cancellation) now run-id idempotency-key]
        inserted? (if idempotency-key
                    (pos? (update! conn (into [(str insert-task-sql
                                                    " on conflict (queue_name, idempotency_key) do nothing")]
                                              task-params)))
                    (do (update! conn (into [insert-task-sql] task-params))
                        true))]
    (if inserted?
      (do (insert-run! conn now queue run-id task-id 1 "pending" now)
          {:task-id task-id
           :run-id run-id
           :attempt 1
           :created true})
      (let [existing (query-one conn ["select task_id, last_attempt_run, attempts from absurd_tasks
                                        where queue_name = ? and idempotency_key = ?"
                                      queue idempotency-key])]
        {:task-id (:task-id existing)
         :run-id (:last-attempt-run existing)
         :attempt (:attempts existing)
         :created false}))))

(defn get-task-result
  "`{:task-id :state :result :failure-reason}` for a task, or nil. The result
  is only set for completed tasks and the failure only for failed ones."
  [conn _now queue task-id]
  (validate-queue-name queue)
  (require-queue! conn queue)
  (when-let [row (query-one conn ["select t.task_id, t.state, t.completed_payload, r.failure_reason
                                     from absurd_tasks t
                                     left join absurd_runs r
                                       on r.queue_name = t.queue_name and r.run_id = t.last_attempt_run
                                    where t.queue_name = ? and t.task_id = ?"
                                  queue (uuid-text task-id)])]
    {:task-id (:task-id row)
     :state (:state row)
     :result (when (= "completed" (:state row))
               (decode-json (:completed-payload row)))
     :failure-reason (when (= "failed" (:state row))
                       (decode-json (:failure-reason row)))}))
