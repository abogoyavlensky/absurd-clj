(ns absurd.test-helpers
  "Shared fixtures and database helpers for the integration test suite.

  `with-conn` runs every test once per backend listed in `ABSURD_TEST_BACKENDS`
  (comma-separated, default `postgres`). Postgres tests run against a
  Testcontainers Postgres started lazily from the JDBC URL, with one real
  connection per test because `absurd.fake_now` is a session-scoped setting.

  `*conn*` is the connectable of the active backend. Row helpers return the
  same shapes on every backend: JSON columns decoded, UUIDs as strings and
  timestamps as `Instant`."
  (:require [absurd.core :as absurd]
            [clojure.string :as str]
            [clojure.test :refer [testing]]
            [jsonista.core :as json]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.sql Connection Timestamp]
           [java.time Instant OffsetDateTime]
           [java.time.format DateTimeFormatter]
           [java.util UUID]
           [org.postgresql.util PGobject]))

(def jdbc-url
  (str "jdbc:tc:postgresql:16-alpine:///absurd"
       "?TC_INITSCRIPT=file:test/resources/absurd.sql&TC_DAEMON=true"))

(def ^:dynamic *backend*
  "The backend of the running test, `:postgres` or `:sqlite`."
  nil)

(def ^:dynamic *conn*
  "The connectable of the running test: a Connection on Postgres."
  nil)

(def ^:dynamic *db-now*
  "An atom holding the pinned engine clock (an `Instant`) or nil. Used on SQLite."
  nil)

(def ^:private row-opts
  {:builder-fn rs/as-unqualified-kebab-maps})

(def ^:private mapper
  (json/object-mapper {:decode-key-fn keyword}))

(def ^:private default-backends "postgres")

(defn- test-backends
  "The backends to run, from `ABSURD_TEST_BACKENDS`. Unknown names throw."
  []
  (let [names (->> (str/split (or (System/getenv "ABSURD_TEST_BACKENDS") default-backends) #",")
                   (map str/trim)
                   (remove str/blank?))]
    (mapv (fn [n]
            (let [backend (keyword n)]
              (when-not (contains? #{:postgres} backend)
                (throw (ex-info (str "Unknown test backend: " n) {:backend n})))
              backend))
          names)))

(defn datasource
  "A DataSource for the active backend's database."
  []
  (jdbc/get-datasource jdbc-url))

(defn- with-postgres
  "Open one connection, bind `*conn*`, reset fake time afterwards."
  [f]
  (with-open [^Connection conn (jdbc/get-connection (datasource))]
    (jdbc/execute! conn ["set timezone to 'UTC'"])
    (try
      (binding [*backend* :postgres
                *conn* conn]
        (f))
      (finally
        (jdbc/execute! conn ["set absurd.fake_now = default"])))))

(defn with-conn
  "Fixture: run the test once per backend, each in its own `testing` context."
  [f]
  (doseq [backend (test-backends)]
    (testing (str "backend " backend)
      (case backend
        :postgres (with-postgres f)))))

(defmacro only
  "Run `body` only when the active backend is `backend`."
  [backend & body]
  `(when (= ~backend *backend*)
     ~@body))

(defn set-fake-now!
  "Pin the database clock of the active backend, or reset it with nil."
  [conn instant]
  (if instant
    (jdbc/execute! conn [(str "set absurd.fake_now = '"
                              (.format DateTimeFormatter/ISO_INSTANT ^Instant instant)
                              "'")])
    (jdbc/execute! conn ["set absurd.fake_now = default"])))

(defn fake-now-fn
  "A clock that follows the pinned database time of the active backend when
  set, else real time. Mirrors the patched `_get_current_time` in the Python
  SDK tests."
  []
  (let [conn *conn*]
    (fn []
      (let [v (:v (jdbc/execute-one!
                    conn ["select current_setting('absurd.fake_now', true) as v"]))]
        (if (str/blank? v)
          (Instant/now)
          (.toInstant (OffsetDateTime/parse v)))))))

(defn client-opts
  "Client options that bind a client to the active backend and its clock."
  []
  {:db *conn*
   :backend *backend*
   :now-fn (fake-now-fn)})

(defn store
  "A `Store` over the active backend. Options are merged over `client-opts`;
  pass `{:db (datasource)}` for a store another thread can use."
  ([] (store {}))
  ([opts] (:store (absurd/client (merge (client-opts) opts)))))

(defn random-queue
  "A fresh queue name such as `sdk_1a2b3c4d`."
  ([] (random-queue "sdk"))
  ([prefix] (str prefix "_" (subs (str (random-uuid)) 0 8))))

(defn test-client
  "A client bound to the active backend on a fresh queue (created), following
  the pinned clock. Options are merged over the defaults; `:queue` overrides
  the random name."
  ([] (test-client {}))
  ([opts]
   (let [queue (or (:queue opts) (random-queue))
         app (absurd/client (merge (client-opts) {:queue queue} opts))]
     (absurd/create-queue! app)
     app)))

(defn ex-type
  "Run `f`; return the `:type` of a thrown ex-info, or nil when nothing threw."
  [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (:type (ex-data e)))))

(defn- decode-value
  [v]
  (cond
    (and (instance? PGobject v)
         (contains? #{"json" "jsonb"} (.getType ^PGobject v)))
    (some-> (.getValue ^PGobject v) (json/read-value mapper))

    (instance? Timestamp v) (.toInstant ^Timestamp v)
    (instance? UUID v) (str v)
    :else v))

(defn- decode-row
  [row]
  (when row
    (update-vals row decode-value)))

(defn count-tasks
  [conn queue]
  (:n (jdbc/execute-one! conn [(str "select count(*) as n from absurd.t_" queue)] row-opts)))

(defn count-checkpoints
  [conn queue task-id]
  (:n (jdbc/execute-one!
        conn [(str "select count(*) as n from absurd.c_" queue " where task_id = ?::uuid") task-id]
        row-opts)))

(defn count-waits
  "Number of registered event waits in a queue."
  [conn queue]
  (:n (jdbc/execute-one! conn [(str "select count(*) as n from absurd.w_" queue)] row-opts)))

(defn get-task
  "Task row with JSON columns decoded, or nil."
  [conn queue task-id]
  (decode-row
    (jdbc/execute-one!
      conn [(str "select * from absurd.t_" queue " where task_id = ?::uuid") task-id]
      row-opts)))

(defn get-run
  "Run row with JSON columns decoded, or nil."
  [conn queue run-id]
  (decode-row
    (jdbc/execute-one!
      conn [(str "select * from absurd.r_" queue " where run_id = ?::uuid") run-id]
      row-opts)))

(defn get-runs
  "All run rows of a task ordered by attempt."
  [conn queue task-id]
  (mapv decode-row
        (jdbc/execute!
          conn [(str "select * from absurd.r_" queue
                     " where task_id = ?::uuid order by attempt") task-id]
          row-opts)))

(defn get-checkpoints
  "All checkpoint rows of a task ordered by name."
  [conn queue task-id]
  (mapv decode-row
        (jdbc/execute!
          conn [(str "select * from absurd.c_" queue
                     " where task_id = ?::uuid order by checkpoint_name") task-id]
          row-opts)))

(defn get-event
  "The stored event row (`:payload`, `:emitted-at`) or nil."
  [conn queue event-name]
  (decode-row
    (jdbc/execute-one!
      conn [(str "select payload, emitted_at from absurd.e_" queue
                 " where event_name = ?") event-name]
      row-opts)))

(defn get-failure
  "Decoded failure payload of a run, or nil."
  [conn queue run-id]
  (:failure-reason (get-run conn queue run-id)))

(defn set-task-headers-json!
  "Overwrite a task's headers with raw JSON text, bypassing validation."
  [conn queue task-id json-text]
  (jdbc/execute! conn [(str "update absurd.t_" queue
                            " set headers = ?::jsonb where task_id = ?::uuid")
                       json-text task-id])
  nil)

(defn delete-task!
  "Remove a task with its runs, checkpoints and waits. Returns the number of
  tasks deleted."
  [conn queue task-id]
  (doseq [prefix ["w_" "c_" "r_"]]
    (jdbc/execute! conn [(str "delete from absurd." prefix queue " where task_id = ?::uuid") task-id]))
  (:next.jdbc/update-count
    (jdbc/execute-one! conn [(str "delete from absurd.t_" queue " where task_id = ?::uuid") task-id])))

(defn sleep-ms
  [ms]
  (Thread/sleep (long ms)))
