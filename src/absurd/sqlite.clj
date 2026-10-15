(ns absurd.sqlite
  "Absurd on SQLite: a `Store` implemented in Clojure over the ported
  procedures in `absurd.sqlite.engine`.

  Use it through `absurd.core/client` (`:db \"jdbc:sqlite:app.db\"`, or a
  DataSource or Connection with `:backend :sqlite`), or build a store directly
  with `store`. The library has no compile-time dependency on the driver: add
  `org.xerial/sqlite-jdbc` to your own dependencies.

  The library never creates the schema on its own. Create it explicitly: call
  `ensure-schema!` at boot, or apply the migration
  `absurd/sqlite/migrations/001-absurd-init.up.sql` (shipped on the classpath)
  with your own migration tool. Operations on a database without the schema
  throw `:absurd/schema-missing`."
  (:require [absurd.sql :as sql]
            [absurd.sqlite.engine :as engine]
            [absurd.sqlite.schema :as schema]
            [absurd.store :as store])
  (:import [java.sql SQLException]
           [java.time Instant]))

(defn ensure-schema!
  "Create the Absurd tables in the database behind `connectable` (a DataSource
  or a Connection with autocommit on) when missing, enable WAL journaling and
  check the stored schema version. Safe to call repeatedly; call it at boot,
  or apply `absurd/sqlite/migrations/001-absurd-init.up.sql` with a migration
  tool instead. Operations on a database without the schema throw
  `:absurd/schema-missing`."
  [connectable]
  (schema/ensure-schema! {:connectable connectable
                          :lock (engine/lock-for connectable)}))

(defn- now-ms
  "The engine clock as epoch milliseconds."
  ^long [now-fn]
  (.toEpochMilli ^Instant (now-fn)))

(def ^:private schema-missing-message
  (str "The absurd SQLite schema is missing; call absurd.sqlite/ensure-schema! at boot or apply the migration "
       schema/migration-resource))

(defn- run-engine
  "Run the engine transaction, mapping sqlite-jdbc's missing-table error to
  `:absurd/schema-missing`. Other exceptions pass through unchanged."
  [this mode f]
  (try
    (engine/run-tx this mode f)
    (catch SQLException e
      (if (some->> (ex-message e) (re-find #"no such table: absurd_"))
        (throw (ex-info schema-missing-message {:type :absurd/schema-missing} e))
        (throw e)))))

(defn- run
  "Run `(f conn now)` in a transaction of `mode`, with AB001 and AB002 mapped
  to the SDK's signals. `now` is the engine clock, read once per operation
  after the store lock, the connection and SQLite's write lock are held, so a
  queued operation never works with a stale timestamp."
  [this mode f]
  (sql/with-task-state-errors
    (run-engine this mode #(f % (now-ms (:now-fn this))))))

(defrecord SqliteStore [connectable lock now-fn]
  store/Store
  (create-queue! [this queue storage-mode]
    (run this :write #(engine/create-queue %1 %2 queue storage-mode)))
  (drop-queue! [this queue]
    (run this :write #(engine/drop-queue %1 %2 queue)))
  (list-queues [this]
    (run this :read #(engine/list-queues %1 %2)))
  (get-queue-policy [this queue]
    (run this :read #(engine/get-queue-policy %1 %2 queue)))
  (set-queue-policy! [this queue policy]
    (run this :write #(engine/set-queue-policy %1 %2 queue policy)))
  (spawn-task! [this queue task-name params options]
    (run this :write #(engine/spawn-task %1 %2 queue task-name params options)))
  (retry-task! [this queue task-id options]
    (run this :write #(engine/retry-task %1 %2 queue task-id options)))
  (cancel-task! [this queue task-id]
    (run this :write #(engine/cancel-task %1 %2 queue task-id)))
  (get-task-result [this queue task-id]
    (run this :read #(engine/get-task-result %1 %2 queue task-id)))
  (claim-tasks! [this queue worker-id claim-timeout qty]
    (run this :write #(engine/claim-tasks %1 %2 queue worker-id (some-> claim-timeout long)
                                          (some-> qty long))))
  (complete-run! [this queue run-id result]
    (run this :write #(engine/complete-run %1 %2 queue run-id result)))
  (fail-run! [this queue run-id failure]
    (run this :write #(engine/fail-run %1 %2 queue run-id failure)))
  (schedule-run-after! [this queue run-id seconds]
    (run this :write #(engine/schedule-run-after %1 %2 queue run-id seconds)))
  (extend-claim! [this queue run-id seconds]
    (run this :write #(engine/extend-claim %1 %2 queue run-id (some-> seconds long))))
  (set-task-checkpoint-state! [this queue task-id checkpoint-name value run-id extend-by]
    (run this :write #(engine/set-task-checkpoint-state %1 %2 queue task-id checkpoint-name value run-id
                                                        (some-> extend-by long))))
  (get-task-checkpoint-state [this queue task-id checkpoint-name]
    (run this :read #(engine/get-task-checkpoint-state %1 %2 queue task-id checkpoint-name)))
  (get-task-checkpoint-states [this queue task-id run-id]
    (run this :read #(engine/get-task-checkpoint-states %1 %2 queue task-id run-id)))
  (await-event [this queue task-id run-id checkpoint-name event-name timeout]
    (run this :write #(engine/await-event %1 %2 queue task-id run-id checkpoint-name event-name
                                          (some-> timeout long))))
  (emit-event! [this queue event-name payload]
    (run this :write #(engine/emit-event %1 %2 queue event-name payload)))
  (current-time [this]
    (engine/ms->instant (now-ms (:now-fn this)))))

(defn store
  "A `Store` over a SQLite DataSource or Connection. A DataSource lends a
  connection per operation; a Connection (autocommit on) is shared. Every
  operation takes the in-process lock of the connectable, which all stores
  over the same object share (see `absurd.sqlite.engine/lock-for`). Options:
  `:db-now-fn`, the engine clock (default `Instant/now`), which plays the role
  of `absurd.current_time()`."
  ([connectable] (store connectable {}))
  ([connectable {:keys [db-now-fn]
                 :or {db-now-fn #(Instant/now)}}]
   (->SqliteStore connectable (engine/lock-for connectable) db-now-fn)))
