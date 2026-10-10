(ns absurd.sql
  "One function per Absurd stored procedure.

  Every function takes a connectable first: anything next.jdbc accepts on the
  JVM, a `postgres.core` connection under let-go. JSON parameters are encoded
  explicitly, known jsonb columns are decoded by name, and UUIDs are returned
  as strings. Postgres error codes AB001 and AB002 are mapped to
  `:absurd/cancelled` and `:absurd/failed-run` signals.

  SQL is written with `?` placeholders; under let-go they are rewritten to
  `$1, $2, ...` because the pgx driver expects numbered parameters."
  (:require #?@(:lg [[clojure.string :as str]
                     [sql.core :as sqlc]]
                :clj [[next.jdbc :as jdbc]
                      [next.jdbc.result-set :as rs]])
            [absurd.json :as json])
  #?(:lg nil :clj (:import [java.sql SQLException Timestamp]
                           [java.time Instant OffsetDateTime])))

;; Execution

#?(:lg
   (defn- dollar-placeholders
     "Rewrite `?` placeholders as `$1`, `$2`, ... for pgx."
     [^String sql]
     (loop [chars (seq sql) n 1 out []]
       (if-let [c (first chars)]
         (if (= \? c)
           (recur (rest chars) (inc n) (conj out (str "$" n)))
           (recur (rest chars) n (conj out c)))
         (apply str out)))))

#?(:lg
   (defn- kebab-keys
     [row]
     (when row
       (into {} (map (fn [[k v]] [(keyword (str/replace (name k) "_" "-")) v])) row))))

(defn- execute-one
  [conn sqlvec]
  #?(:lg (let [sqlvec (update sqlvec 0 dollar-placeholders)
               row (sqlc/execute-one! conn sqlvec)]
           (when-not (contains? row :sql/update-count)
             (kebab-keys row)))
     :clj (jdbc/execute-one! conn sqlvec {:builder-fn rs/as-unqualified-kebab-maps})))

(defn- execute
  [conn sqlvec]
  #?(:lg (let [sqlvec (update sqlvec 0 dollar-placeholders)
               rows (sqlc/execute! conn sqlvec)]
           (if (and (= 1 (count rows)) (contains? (first rows) :sql/update-count))
             []
             (mapv kebab-keys rows)))
     :clj (jdbc/execute! conn sqlvec {:builder-fn rs/as-unqualified-kebab-maps})))

;; Signals

(defn signal-for-error
  "The SDK signal for a database error, or nil when it is not AB001/AB002.
  Public because `with-task-state-errors` expands to it."
  [e]
  (let [sqlstate #?(:lg (second (re-find #"SQLSTATE (AB00[12])" (or (ex-message e) "")))
                    :clj (when (instance? SQLException e) (.getSQLState ^SQLException e)))]
    (case sqlstate
      "AB001" (ex-info "Task has been cancelled" {:type :absurd/cancelled} e)
      "AB002" (ex-info "Run has already failed" {:type :absurd/failed-run} e)
      nil)))

(defmacro with-task-state-errors
  "Rethrow AB001/AB002 database errors as SDK signals."
  [& body]
  `(try
     ~@body
     (catch #?(:lg Exception :clj java.sql.SQLException) e#
       (throw (or (signal-for-error e#) e#)))))

;; Value helpers

(defn- ->str
  [v]
  (some-> v str))

(defn- ->instant
  "Normalize a timestamp column: `Instant` on the JVM, Go `time.Time` under let-go."
  [v]
  #?(:lg v
     :clj (cond
            (instance? Timestamp v) (.toInstant ^Timestamp v)
            (instance? OffsetDateTime v) (.toInstant ^OffsetDateTime v)
            (instance? Instant v) v
            :else v)))

;; Queues

(defn create-queue!
  "Create a queue. `storage-mode` is \"partitioned\", \"unpartitioned\" or nil."
  [conn queue storage-mode]
  (if storage-mode
    (execute-one conn ["select absurd.create_queue(?, ?)" queue storage-mode])
    (execute-one conn ["select absurd.create_queue(?)" queue]))
  nil)

(defn drop-queue!
  [conn queue]
  (execute-one conn ["select absurd.drop_queue(?)" queue])
  nil)

(defn list-queues
  "Queue names as a vector of strings."
  [conn]
  (mapv :queue-name (execute conn ["select queue_name from absurd.list_queues()"])))

(defn get-queue-policy
  "Queue policy map with interval fields as strings, or nil."
  [conn queue]
  (execute-one
    conn
    ["select queue_name, storage_mode,
             partition_lookahead::text as partition_lookahead,
             partition_lookback::text as partition_lookback,
             cleanup_ttl::text as cleanup_ttl,
             cleanup_limit,
             detach_mode,
             detach_min_age::text as detach_min_age
      from absurd.get_queue_policy(?)" queue]))

(defn set-queue-policy!
  "`policy` is a map with snake_case string keys."
  [conn queue policy]
  (execute-one conn ["select absurd.set_queue_policy(?, ?::jsonb)" queue (json/->jsonb policy)])
  nil)

;; Tasks

(defn- spawn-row
  [row]
  (-> row
      (update :task-id ->str)
      (update :run-id ->str)))

(defn spawn-task!
  "`options` is a map with snake_case string keys."
  [conn queue task-name params options]
  (spawn-row
    (execute-one conn ["select task_id, run_id, attempt, created
                        from absurd.spawn_task(?, ?, ?::jsonb, ?::jsonb)"
                       queue task-name (json/->jsonb params) (json/->jsonb options)])))

(defn retry-task!
  "`options` is a map with snake_case string keys."
  [conn queue task-id options]
  (spawn-row
    (execute-one conn ["select task_id, run_id, attempt, created
                        from absurd.retry_task(?, ?::uuid, ?::jsonb)"
                       queue task-id (json/->jsonb options)])))

(defn cancel-task!
  [conn queue task-id]
  (execute-one conn ["select absurd.cancel_task(?, ?::uuid)" queue task-id])
  nil)

(defn get-task-result
  "Map with `:task-id`, `:state`, `:result`, `:failure-reason`, or nil."
  [conn queue task-id]
  (some-> (execute-one conn ["select task_id, state, result, failure_reason
                              from absurd.get_task_result(?, ?::uuid)" queue task-id])
          (update :task-id ->str)
          (update :result json/<-jsonb)
          (update :failure-reason json/<-jsonb)))

;; Runs

(defn claim-tasks!
  "Claim up to `qty` tasks. Returns a vector of claim maps."
  [conn queue worker-id claim-timeout qty]
  (mapv (fn [row]
          (-> row
              (update :run-id ->str)
              (update :task-id ->str)
              (update :params json/<-jsonb)
              (update :retry-strategy json/<-jsonb)
              (update :headers json/<-jsonb)
              (update :event-payload json/<-jsonb)))
        (execute conn ["select run_id, task_id, attempt, task_name, params, retry_strategy,
                               max_attempts, headers, wake_event, event_payload
                        from absurd.claim_task(?, ?, ?::integer, ?::integer)"
                       queue worker-id (int claim-timeout) (int qty)])))

(defn complete-run!
  [conn queue run-id result]
  (with-task-state-errors
    (execute-one conn ["select absurd.complete_run(?, ?::uuid, ?::jsonb)"
                       queue run-id (json/->jsonb result)]))
  nil)

(defn fail-run!
  "`failure` is the JSON failure payload map."
  [conn queue run-id failure]
  (with-task-state-errors
    (execute-one conn ["select absurd.fail_run(?, ?::uuid, ?::jsonb)"
                       queue run-id (json/->jsonb failure)]))
  nil)

(defn schedule-run-after!
  "Reschedule a run `seconds` after the database clock."
  [conn queue run-id seconds]
  (execute-one conn ["select absurd.schedule_run(?, ?::uuid,
                        absurd.current_time() + make_interval(secs => ?::double precision))"
                     queue run-id (double seconds)])
  nil)

(defn extend-claim!
  [conn queue run-id seconds]
  (with-task-state-errors
    (execute-one conn ["select absurd.extend_claim(?, ?::uuid, ?::integer)"
                       queue run-id (int seconds)]))
  nil)

;; Checkpoints

(defn- checkpoint-row
  [row]
  (-> row
      (update :state json/<-jsonb)
      (update :owner-run-id ->str)
      (update :updated-at ->instant)))

(defn set-task-checkpoint-state!
  [conn queue task-id checkpoint-name value run-id extend-by]
  (with-task-state-errors
    (execute-one conn ["select absurd.set_task_checkpoint_state(?, ?::uuid, ?, ?::jsonb, ?::uuid, ?::integer)"
                       queue task-id checkpoint-name (json/->jsonb value) run-id
                       (some-> extend-by int)]))
  nil)

(defn get-task-checkpoint-state
  "One checkpoint row or nil."
  [conn queue task-id checkpoint-name]
  (some-> (execute-one conn ["select checkpoint_name, state, status, owner_run_id, updated_at
                              from absurd.get_task_checkpoint_state(?, ?::uuid, ?)"
                             queue task-id checkpoint-name])
          checkpoint-row))

(defn get-task-checkpoint-states
  "All checkpoint rows visible to a run."
  [conn queue task-id run-id]
  (mapv checkpoint-row
        (execute conn ["select checkpoint_name, state, status, owner_run_id, updated_at
                        from absurd.get_task_checkpoint_states(?, ?::uuid, ?::uuid)"
                       queue task-id run-id])))

;; Events

(defn await-event
  "Register a wait. Returns `{:should-suspend bool :payload v}`."
  [conn queue task-id run-id checkpoint-name event-name timeout]
  (with-task-state-errors
    (-> (execute-one conn ["select should_suspend, payload
                            from absurd.await_event(?, ?::uuid, ?::uuid, ?, ?, ?::integer)"
                           queue task-id run-id checkpoint-name event-name
                           (some-> timeout int)])
        (update :payload json/<-jsonb))))

(defn emit-event!
  [conn queue event-name payload]
  (execute-one conn ["select absurd.emit_event(?, ?, ?::jsonb)" queue event-name (json/->jsonb payload)])
  nil)

;; Time

(defn current-time
  "The database clock, honouring `absurd.fake_now`: an `Instant` on the JVM,
  a Go `time.Time` under let-go."
  [conn]
  (->instant (:t (execute-one conn ["select absurd.current_time() as t"]))))
