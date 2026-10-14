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

(defn- cast-integer
  "Cast a JSON value to an integer of type `type-name` within [lo, hi], like
  `(v->>'k')::<type>`: nil stays nil, anything else that is not an integer
  raises."
  [v type-name lo hi]
  (when-let [text (json-text v)]
    (if (re-matches #"\s*[-+]?\d+\s*" text)
      (let [n (BigInteger. (str/trim text))]
        (if (<= lo n hi)
          (long n)
          (raise! "22003" (str "value \"" (str/trim text) "\" is out of range for type " type-name))))
      (raise! "22P02" (str "invalid input syntax for type " type-name ": \"" text "\"")))))

(defn- int-text
  "Cast to a 32-bit `integer`."
  [v]
  (cast-integer v "integer" Integer/MIN_VALUE Integer/MAX_VALUE))

(defn- bigint-text
  "Cast to a 64-bit `bigint`."
  [v]
  (cast-integer v "bigint" Long/MIN_VALUE Long/MAX_VALUE))

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

(defn set-queue-policy
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

;; Cancellation

(defn cancel-task
  "Cancel a task and its unfinished runs and clear its event waits. A task
  that already finished is left alone; an unknown task raises."
  [conn now queue task-id]
  (let [task-id (uuid-text task-id)
        state (:state (query-one conn ["select state from absurd_tasks
                                         where queue_name = ? and task_id = ?" queue task-id]))]
    (when-not state
      (raise! (str "Task \"" task-id "\" not found in queue \"" queue "\"")))
    (when-not (contains? #{"completed" "failed" "cancelled"} state)
      (update! conn ["update absurd_tasks
                         set state = 'cancelled', cancelled_at = coalesce(cancelled_at, ?)
                       where queue_name = ? and task_id = ?" now queue task-id])
      (update! conn ["update absurd_runs
                         set state = 'cancelled', claimed_by = null, claim_expires_at = null
                       where queue_name = ? and task_id = ?
                         and state not in ('completed', 'failed', 'cancelled')" queue task-id])
      (update! conn ["delete from absurd_waits where queue_name = ? and task_id = ?" queue task-id]))
    nil))

;; Run lifecycle

(defn- raise-run-state!
  "Raise the error for a run in an unexpected `state`: AB001 for cancelled,
  AB002 for failed, else the generic message built from `generic`."
  [state run-id queue generic]
  (case state
    "cancelled" (raise! "AB001" "Task has been cancelled")
    "failed" (raise! "AB002" (str "Run \"" run-id "\" has already failed in queue \"" queue "\""))
    (raise! generic)))

(defn complete-run
  "Mark a running run, and its task, completed with `result`."
  [conn now queue run-id result]
  (let [run-id (uuid-text run-id)
        run (query-one conn ["select task_id, state from absurd_runs
                               where queue_name = ? and run_id = ?" queue run-id])]
    (when-not run
      (raise! (str "Run \"" run-id "\" not found in queue \"" queue "\"")))
    (when (not= "running" (:state run))
      (raise-run-state! (:state run) run-id queue
                        (str "Run \"" run-id "\" is not currently running in queue \"" queue "\"")))
    (let [payload (encode-json result)]
      (update! conn ["update absurd_runs set state = 'completed', completed_at = ?, result = ?
                       where queue_name = ? and run_id = ?" now payload queue run-id])
      (update! conn ["update absurd_tasks
                         set state = 'completed', completed_payload = ?, last_attempt_run = ?
                       where queue_name = ? and task_id = ?" payload run-id queue (:task-id run)])
      (update! conn ["delete from absurd_waits where queue_name = ? and run_id = ?" queue run-id]))
    nil))

(defn schedule-run
  "Put a running run to sleep until `wake-at` (epoch milliseconds)."
  [conn _now queue run-id wake-at]
  (let [run-id (uuid-text run-id)
        run (query-one conn ["select task_id from absurd_runs
                               where queue_name = ? and run_id = ? and state = 'running'"
                             queue run-id])]
    (when-not run
      (raise! (str "Run \"" run-id "\" is not currently running in queue \"" queue "\"")))
    (update! conn ["update absurd_runs
                       set state = 'sleeping', claimed_by = null, claim_expires_at = null,
                           available_at = ?, wake_event = null
                     where queue_name = ? and run_id = ?" wake-at queue run-id])
    (update! conn ["update absurd_tasks set state = 'sleeping' where queue_name = ? and task_id = ?"
                   queue (:task-id run)])
    nil))

(defn- seconds->ms
  ^long [seconds]
  (Math/round (* (double seconds) 1000.0)))

(defn schedule-run-after
  "Put a running run to sleep for `seconds` after the engine clock."
  [conn now queue run-id seconds]
  (schedule-run conn now queue run-id (+ now (seconds->ms seconds))))

(defn fail-run
  "Fail a run and, while attempts remain, schedule the next one after the retry
  strategy's delay. A task that cannot retry fails, or is cancelled when the
  retry would pass `max_duration`."
  [conn now queue run-id reason]
  (let [run-id (uuid-text run-id)
        run (query-one conn ["select task_id, attempt, state from absurd_runs
                               where queue_name = ? and run_id = ?" queue run-id])
        cannot-fail (str "Run \"" run-id "\" cannot be failed in queue \"" queue "\"")]
    (when-not run
      (raise! cannot-fail))
    (when (not (contains? #{"running" "sleeping"} (:state run)))
      (raise-run-state! (:state run) run-id queue cannot-fail))
    (let [{:keys [task-id attempt]} run
          task (query-one conn ["select retry_strategy, max_attempts, first_started_at, cancellation
                                   from absurd_tasks where queue_name = ? and task_id = ?"
                                queue task-id])
          next-attempt (inc attempt)]
      (update! conn ["update absurd_runs
                         set state = 'failed', wake_event = null, failed_at = ?, failure_reason = ?
                       where queue_name = ? and run_id = ?" now (encode-json reason) queue run-id])
      (let [retry? (or (nil? (:max-attempts task)) (<= next-attempt (:max-attempts task)))
            ;; Legacy tasks may hold a strategy that predates validation. If it
            ;; is invalid, fail the task permanently rather than wedging the queue.
            next-available (when retry?
                             (try
                               (+ now (seconds->ms (retry-delay-seconds (decode-json (:retry-strategy task))
                                                                        attempt)))
                               (catch SQLException e
                                 (if (= "AB003" (.getSQLState e)) nil (throw e)))))
            max-duration (some-> (decode-json (:cancellation task)) :max_duration bigint-text)
            cancel? (boolean (and next-available max-duration (:first-started-at task)
                                  (>= (- next-available (:first-started-at task))
                                      (*' max-duration 1000))))
            new-run-id (when (and next-available (not cancel?)) (uuidv7 now))
            state-after (cond cancel? "cancelled"
                              new-run-id (if (> next-available now) "sleeping" "pending")
                              :else "failed")]
        (when new-run-id
          (insert-run! conn now queue new-run-id task-id next-attempt state-after next-available))
        (update! conn ["update absurd_tasks
                           set state = ?, attempts = max(attempts, ?), last_attempt_run = ?,
                               cancelled_at = coalesce(cancelled_at, ?)
                         where queue_name = ? and task_id = ?"
                       state-after (if new-run-id next-attempt attempt) (or new-run-id run-id)
                       (when cancel? now) queue task-id])
        (update! conn ["delete from absurd_waits where queue_name = ? and run_id = ?" queue run-id])
        nil))))

(defn extend-claim
  "Push a running run's lease `seconds` past the engine clock."
  [conn now queue run-id seconds]
  (when (or (nil? seconds) (<= seconds 0))
    (raise! "extend_by must be > 0"))
  (let [run-id (uuid-text run-id)
        run (query-one conn ["select r.state, r.claim_expires_at, t.state as task_state
                                from absurd_runs r
                                join absurd_tasks t on t.queue_name = r.queue_name and t.task_id = r.task_id
                               where r.queue_name = ? and r.run_id = ?" queue run-id])]
    (when-not run
      (raise! (str "Run \"" run-id "\" not found in queue \"" queue "\"")))
    (when (= "cancelled" (:task-state run))
      (raise! "AB001" "Task has been cancelled"))
    (when (not= "running" (:state run))
      (raise-run-state! (:state run) run-id queue
                        (str "Run \"" run-id "\" is not currently running in queue \"" queue "\"")))
    (when-not (:claim-expires-at run)
      (raise! (str "Run \"" run-id "\" does not have an active claim in queue \"" queue "\"")))
    (update! conn ["update absurd_runs set claim_expires_at = ? where queue_name = ? and run_id = ?"
                   (+ now (* seconds 1000)) queue run-id])
    nil))

;; Claiming

(defn- cancel-overdue-tasks!
  "Apply `max_delay` (task never started) and `max_duration` (task started)
  cancellation rules, using `cancel-task` so the run and task stay in step."
  [conn now queue]
  (doseq [{:keys [task-id]}
          (query conn ["select task_id from absurd_tasks
                         where queue_name = ?
                           and state in ('pending', 'sleeping', 'running')
                           and cancellation is not null
                           and ((json_extract(cancellation, '$.max_delay') is not null
                                 and first_started_at is null
                                 and ? - enqueue_at >= json_extract(cancellation, '$.max_delay') * 1000)
                             or (json_extract(cancellation, '$.max_duration') is not null
                                 and first_started_at is not null
                                 and ? - first_started_at >= json_extract(cancellation, '$.max_duration') * 1000))
                         order by task_id" queue now now])]
    (cancel-task conn now queue task-id)))

(defn- fail-expired-leases!
  "Fail up to `limit` runs whose lease ran out, as `$ClaimTimeout`."
  [conn now queue limit]
  (doseq [{:keys [run-id claimed-by claim-expires-at attempt]}
          (query conn ["select run_id, claimed_by, claim_expires_at, attempt from absurd_runs
                         where queue_name = ? and state = 'running'
                           and claim_expires_at is not null and claim_expires_at <= ?
                         order by claim_expires_at, run_id
                         limit ?" queue now limit])]
    (fail-run conn now queue run-id
              (cond-> {:name "$ClaimTimeout"
                       :message "worker did not finish task within claim interval"
                       :claimExpiredAt (str (ms->instant claim-expires-at))
                       :attempt attempt}
                claimed-by (assoc :workerId claimed-by)))))

(defn claim-tasks
  "Claim up to `qty` runs for `worker-id` with a lease of `claim-timeout`
  seconds (no lease when 0). First applies cancellation rules and fails runs
  whose lease expired (at most `qty` of them), then claims the oldest due
  runs. Returns the claim maps ordered by `available_at`, then run id."
  [conn now queue worker-id claim-timeout qty]
  (require-queue! conn queue)
  (let [timeout (max (or claim-timeout 30) 0)
        worker-id (if (str/blank? worker-id) "worker" worker-id)
        qty (max (or qty 1) 1)
        claim-until (when (pos? timeout) (+ now (* timeout 1000)))]
    (cancel-overdue-tasks! conn now queue)
    (fail-expired-leases! conn now queue qty)
    (let [rows (query conn ["select r.run_id, r.task_id, r.attempt, t.task_name, t.params,
                                    t.retry_strategy, t.max_attempts, t.headers,
                                    r.wake_event, r.event_payload
                               from absurd_runs r
                               join absurd_tasks t on t.queue_name = r.queue_name and t.task_id = r.task_id
                              where r.queue_name = ?
                                and r.state in ('pending', 'sleeping')
                                and t.state in ('pending', 'sleeping', 'running')
                                and r.available_at <= ?
                              order by r.available_at, r.run_id
                              limit ?" queue now qty])]
      (doseq [{:keys [run-id task-id attempt]} rows]
        (update! conn ["update absurd_runs
                           set state = 'running', claimed_by = ?, claim_expires_at = ?,
                               started_at = ?, available_at = ?
                         where queue_name = ? and run_id = ?"
                       worker-id claim-until now now queue run-id])
        (update! conn ["update absurd_tasks
                           set state = 'running', attempts = max(attempts, ?),
                               first_started_at = coalesce(first_started_at, ?), last_attempt_run = ?
                         where queue_name = ? and task_id = ?" attempt now run-id queue task-id])
        (update! conn ["delete from absurd_waits
                         where queue_name = ? and run_id = ? and timeout_at is not null and timeout_at <= ?"
                       queue run-id now]))
      (mapv (fn [row]
              (-> row
                  (update :params decode-json)
                  (update :retry-strategy decode-json)
                  (update :headers decode-json)
                  (update :event-payload decode-json)))
            rows))))

;; Retry

(defn- strip-nils
  "Drop object fields whose value is nil, recursively, like `jsonb_strip_nulls`."
  [v]
  (cond
    (map? v) (into {} (keep (fn [[k x]] (when (some? x) [k (strip-nils x)]))) v)
    (vector? v) (mapv strip-nils v)
    :else v))

(defn- boolean-text
  "Cast a JSON value to a boolean like `(v->>'k')::boolean`."
  [v]
  (when-let [text (json-text v)]
    (let [t (str/lower-case (str/trim text))]
      (cond
        (contains? #{"true" "t" "yes" "y" "on" "1"} t) true
        (contains? #{"false" "f" "no" "n" "off" "0"} t) false
        :else (raise! "22P02" (str "invalid input syntax for type boolean: \"" text "\""))))))

(defn retry-task
  "Retry a failed task, either in place (a new run with attempt + 1) or, with
  `spawn_new`, as a new task copied from the original inputs. `options` takes
  `spawn_new` and `max_attempts`."
  [conn now queue task-id options]
  (let [options (let [o (normalize options)] (if (map? o) o {}))
        spawn-new? (boolean (boolean-text (:spawn_new options)))
        requested (when (contains? options :max_attempts)
                    (int-text (:max_attempts options)))
        _ (when (and requested (< requested 1))
            (raise! "max_attempts must be >= 1"))
        task-id (uuid-text task-id)
        task (query-one conn ["select task_name, params, headers, retry_strategy, max_attempts,
                                      cancellation, attempts, state
                                 from absurd_tasks where queue_name = ? and task_id = ?" queue task-id])]
    (when-not task
      (raise! (str "Task \"" task-id "\" not found in queue \"" queue "\"")))
    (when (not= "failed" (:state task))
      (raise! (str "Task \"" task-id "\" is not currently failed in queue \"" queue "\"")))
    (if spawn-new?
      (spawn-task conn now queue (:task-name task) (decode-json (:params task))
                  (strip-nils {:headers (decode-json (:headers task))
                               :retry_strategy (decode-json (:retry-strategy task))
                               :max_attempts (or requested (:max-attempts task))
                               :cancellation (decode-json (:cancellation task))}))
      (let [attempts (:attempts task)
            max-attempts (or requested (inc (or (:max-attempts task) attempts)))]
        (when (<= max-attempts attempts)
          (raise! (str "max_attempts (" max-attempts ") must be greater than current attempts ("
                       attempts ")")))
        (let [run-id (uuidv7 now)
              attempt (inc attempts)]
          (insert-run! conn now queue run-id task-id attempt "pending" now)
          (update! conn ["update absurd_tasks
                             set state = 'pending', attempts = max(attempts, ?), max_attempts = ?,
                                 last_attempt_run = ?, cancelled_at = null
                           where queue_name = ? and task_id = ?" attempt max-attempts run-id queue task-id])
          {:task-id task-id
           :run-id run-id
           :attempt attempt
           :created false})))))

;; Checkpoints

(def ^:private upsert-checkpoint-sql
  "insert into absurd_checkpoints (queue_name, task_id, checkpoint_name, state, status, owner_run_id, updated_at)
   values (?, ?, ?, ?, 'committed', ?, ?)
   on conflict (queue_name, task_id, checkpoint_name)
   do update set state = excluded.state, status = excluded.status,
                 owner_run_id = excluded.owner_run_id, updated_at = excluded.updated_at")

(defn- upsert-checkpoint!
  "Write a committed checkpoint whose `state` is already JSON text."
  [conn now queue task-id step-name state run-id]
  (update! conn [upsert-checkpoint-sql queue task-id step-name state run-id now]))

(defn- checkpoint-row
  [row]
  (-> row
      (update :state decode-json)
      (update :updated-at ms->instant)))

(defn set-task-checkpoint-state
  "Save a step result. A checkpoint owned by a later attempt is not
  overwritten by an earlier one. With `extend-by` > 0 the run's lease is
  extended as well. Raises AB001 for a cancelled task and AB002 for a failed
  run."
  [conn now queue task-id step-name value run-id extend-by]
  (when (or (nil? step-name) (str/blank? step-name))
    (raise! "step_name must be provided"))
  (let [task-id (uuid-text task-id)
        run-id (uuid-text run-id)
        run (query-one conn ["select r.attempt, r.state, t.state as task_state
                                from absurd_runs r
                                join absurd_tasks t on t.queue_name = r.queue_name and t.task_id = r.task_id
                               where r.queue_name = ? and r.run_id = ?" queue run-id])]
    (when-not run
      (raise! (str "Run \"" run-id "\" not found for checkpoint")))
    (when (= "cancelled" (:task-state run))
      (raise! "AB001" "Task has been cancelled"))
    (when (= "failed" (:state run))
      (raise! "AB002" (str "Run \"" run-id "\" has already failed in queue \"" queue "\"")))
    (when (and extend-by (pos? extend-by))
      (update! conn ["update absurd_runs set claim_expires_at = ?
                       where queue_name = ? and run_id = ? and state = 'running'
                         and claim_expires_at is not null"
                     (+ now (* extend-by 1000)) queue run-id]))
    (let [existing (query-one conn ["select c.owner_run_id, r.attempt
                                       from absurd_checkpoints c
                                       left join absurd_runs r
                                         on r.queue_name = c.queue_name and r.run_id = c.owner_run_id
                                      where c.queue_name = ? and c.task_id = ? and c.checkpoint_name = ?"
                                    queue task-id step-name])]
      (when (or (nil? (:owner-run-id existing))
                (nil? (:attempt existing))
                (>= (:attempt run) (:attempt existing)))
        (upsert-checkpoint! conn now queue task-id step-name (encode-json value) run-id))
      nil)))

(defn get-task-checkpoint-state
  "One committed checkpoint row, or nil."
  [conn _now queue task-id step-name]
  (some-> (query-one conn ["select checkpoint_name, state, status, owner_run_id, updated_at
                              from absurd_checkpoints
                             where queue_name = ? and task_id = ? and checkpoint_name = ?
                               and status = 'committed'" queue (uuid-text task-id) step-name])
          checkpoint-row))

(defn get-task-checkpoint-states
  "The committed checkpoints visible to a run, oldest first. Checkpoints from
  later attempts are hidden."
  [conn _now queue task-id run-id]
  (let [task-id (uuid-text task-id)
        run-id (uuid-text run-id)
        run (query-one conn ["select task_id, attempt from absurd_runs
                               where queue_name = ? and run_id = ?" queue run-id])]
    (when-not run
      (raise! (str "Run \"" run-id "\" not found in queue \"" queue "\"")))
    (when (not= task-id (:task-id run))
      (raise! (str "Run \"" run-id "\" does not belong to task \"" task-id "\" in queue \"" queue "\"")))
    (mapv checkpoint-row
          (query conn ["select c.checkpoint_name, c.state, c.status, c.owner_run_id, c.updated_at
                          from absurd_checkpoints c
                          left join absurd_runs owner_run
                            on owner_run.queue_name = c.queue_name and owner_run.run_id = c.owner_run_id
                         where c.queue_name = ? and c.task_id = ? and c.status = 'committed'
                           and (owner_run.attempt is null or owner_run.attempt <= ?)
                         order by c.updated_at, c.rowid" queue task-id (:attempt run)]))))

;; Events

(defn await-event
  "Wait for an event: resolve at once from an existing checkpoint or emitted
  event, or put the run to sleep until the event or `timeout` seconds pass.
  Returns `{:should-suspend bool :payload v}`.

  A run resumed by its timeout has `wake_event` set and no payload; that case
  returns `{:should-suspend false :payload nil}` without registering the wait
  again."
  [conn now queue task-id run-id step-name event-name timeout]
  (when (or (nil? event-name) (str/blank? event-name))
    (raise! "event_name must be provided"))
  (when (and timeout (neg? timeout))
    (raise! "timeout must be non-negative"))
  (let [task-id (uuid-text task-id)
        run-id (uuid-text run-id)
        timeout-at (when timeout (+ now (* timeout 1000)))
        checkpoint (query-one conn ["select state from absurd_checkpoints
                                      where queue_name = ? and task_id = ? and checkpoint_name = ?"
                                    queue task-id step-name])]
    (if (some? (:state checkpoint))
      {:should-suspend false
       :payload (decode-json (:state checkpoint))}
      (do
        ;; A row with a NULL payload marks "not emitted yet"; emit-event always
        ;; writes non-NULL JSON, at minimum the text null.
        (update! conn ["insert or ignore into absurd_events (queue_name, event_name, payload, emitted_at)
                        values (?, ?, null, 0)" queue event-name])
        (let [run (query-one conn ["select r.state, r.event_payload, r.wake_event, t.state as task_state
                                      from absurd_runs r
                                      join absurd_tasks t on t.queue_name = r.queue_name and t.task_id = r.task_id
                                     where r.queue_name = ? and r.run_id = ?" queue run-id])]
          (when-not run
            (raise! (str "Run \"" run-id "\" not found while awaiting event")))
          (when (= "cancelled" (:task-state run))
            (raise! "AB001" "Task has been cancelled"))
          (let [event-payload (:payload (query-one conn ["select payload from absurd_events
                                                           where queue_name = ? and event_name = ?"
                                                         queue event-name]))
                existing (:event-payload run)
                _ (when existing
                    (update! conn ["update absurd_runs set event_payload = null
                                     where queue_name = ? and run_id = ?" queue run-id]))
                resolved (when (and existing event-payload
                                    (= (decode-json event-payload) (decode-json existing)))
                           existing)]
            (when (not= "running" (:state run))
              (raise! (str "Run \"" run-id "\" must be running to await events")))
            (let [resolved (or resolved event-payload)]
              (cond
                resolved
                (do (upsert-checkpoint! conn now queue task-id step-name resolved run-id)
                    {:should-suspend false
                     :payload (decode-json resolved)})

                ;; Resumed by the timeout: do not sleep again or register a new wait.
                (and (= event-name (:wake-event run)) (nil? existing))
                (do (update! conn ["update absurd_runs set wake_event = null
                                     where queue_name = ? and run_id = ?" queue run-id])
                    {:should-suspend false
                     :payload nil})

                :else
                (do (update! conn ["insert into absurd_waits (queue_name, task_id, run_id, step_name,
                                                              event_name, timeout_at, created_at)
                                    values (?, ?, ?, ?, ?, ?, ?)
                                    on conflict (queue_name, run_id, step_name)
                                    do update set event_name = excluded.event_name,
                                                  timeout_at = excluded.timeout_at,
                                                  created_at = excluded.created_at"
                                   queue task-id run-id step-name event-name timeout-at now])
                    (update! conn ["update absurd_runs
                                       set state = 'sleeping', claimed_by = null, claim_expires_at = null,
                                           available_at = ?, wake_event = ?, event_payload = null
                                     where queue_name = ? and run_id = ?"
                                   (or timeout-at infinity) event-name queue run-id])
                    (update! conn ["update absurd_tasks set state = 'sleeping'
                                     where queue_name = ? and task_id = ?" queue task-id])
                    {:should-suspend true
                     :payload nil})))))))))

(defn emit-event
  "Emit an event. The first emit per name wins; later ones change nothing.
  Waiters whose timeout has not passed wake up with the payload and get it
  saved as their step checkpoint; waits that already timed out are dropped."
  [conn now queue event-name payload]
  (when (or (nil? event-name) (str/blank? event-name))
    (raise! "event_name must be provided"))
  (require-queue! conn queue)
  (let [payload (encode-json payload)
        applied (update! conn ["insert into absurd_events (queue_name, event_name, payload, emitted_at)
                                values (?, ?, ?, ?)
                                on conflict (queue_name, event_name)
                                do update set payload = excluded.payload,
                                              emitted_at = excluded.emitted_at
                                 where absurd_events.payload is null"
                               queue event-name payload now])]
    (when (pos? applied)
      (update! conn ["delete from absurd_waits
                       where queue_name = ? and event_name = ?
                         and timeout_at is not null and timeout_at <= ?" queue event-name now])
      (doseq [{:keys [run-id task-id step-name]}
              (query conn ["select w.run_id, w.task_id, w.step_name
                              from absurd_waits w
                              join absurd_runs r on r.queue_name = w.queue_name and r.run_id = w.run_id
                             where w.queue_name = ? and w.event_name = ? and r.state = 'sleeping'"
                           queue event-name])]
        (update! conn ["update absurd_runs
                           set state = 'pending', available_at = ?, wake_event = null,
                               event_payload = ?, claimed_by = null, claim_expires_at = null
                         where queue_name = ? and run_id = ?" now payload queue run-id])
        (upsert-checkpoint! conn now queue task-id step-name payload run-id)
        (update! conn ["update absurd_tasks set state = 'pending'
                         where queue_name = ? and task_id = ?" queue task-id])
        (update! conn ["delete from absurd_waits
                         where queue_name = ? and event_name = ? and run_id = ?"
                       queue event-name run-id])))
    nil))
