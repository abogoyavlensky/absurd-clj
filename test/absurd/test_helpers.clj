(ns absurd.test-helpers
  "Shared fixtures and database helpers for the integration test suite.

  Tests run against a Testcontainers Postgres started lazily from the JDBC
  URL. Each test gets one real connection because `absurd.fake_now` is a
  session-scoped setting."
  (:require [absurd.core :as absurd]
            [clojure.string :as str]
            [jsonista.core :as json]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.sql Connection]
           [java.time Instant OffsetDateTime]
           [java.time.format DateTimeFormatter]
           [org.postgresql.util PGobject]))

(def jdbc-url
  (str "jdbc:tc:postgresql:16-alpine:///absurd"
       "?TC_INITSCRIPT=file:test/resources/absurd.sql&TC_DAEMON=true"))

(def ^:dynamic *conn* nil)

(def ^:private row-opts
  {:builder-fn rs/as-unqualified-kebab-maps})

(def ^:private mapper
  (json/object-mapper {:decode-key-fn keyword}))

(defn datasource
  "A DataSource for the shared test container."
  []
  (jdbc/get-datasource jdbc-url))

(defn with-conn
  "Fixture: open one connection, bind `*conn*`, reset fake time afterwards."
  [f]
  (with-open [^Connection conn (jdbc/get-connection (datasource))]
    (jdbc/execute! conn ["set timezone to 'UTC'"])
    (try
      (binding [*conn* conn]
        (f))
      (finally
        (jdbc/execute! conn ["set absurd.fake_now = default"])))))

(defn set-fake-now!
  "Pin `absurd.current_time()` for this session, or reset it with nil."
  [conn instant]
  (if instant
    (jdbc/execute! conn [(str "set absurd.fake_now = '"
                              (.format DateTimeFormatter/ISO_INSTANT ^Instant instant)
                              "'")])
    (jdbc/execute! conn ["set absurd.fake_now = default"])))

(defn fake-now-fn
  "A clock that follows `absurd.fake_now` when set, else real time.
  Mirrors the patched `_get_current_time` in the Python SDK tests."
  [conn]
  (fn []
    (let [v (:v (jdbc/execute-one!
                  conn ["select current_setting('absurd.fake_now', true) as v"]))]
      (if (str/blank? v)
        (Instant/now)
        (.toInstant (OffsetDateTime/parse v))))))

(defn random-queue
  "A fresh queue name such as `sdk_1a2b3c4d`."
  ([] (random-queue "sdk"))
  ([prefix] (str prefix "_" (subs (str (random-uuid)) 0 8))))

(defn test-client
  "A client bound to `*conn*` on a fresh queue (created), following `fake_now`.
  Options are merged over the defaults; `:queue` overrides the random name."
  ([] (test-client {}))
  ([opts]
   (let [queue (or (:queue opts) (random-queue))
         app (absurd/client (merge {:db *conn*
                                    :queue queue
                                    :now-fn (fake-now-fn *conn*)}
                                   opts))]
     (absurd/create-queue! app)
     app)))

(defn count-tasks
  [conn queue]
  (:n (jdbc/execute-one! conn [(str "select count(*) as n from absurd.t_" queue)] row-opts)))

(defn count-checkpoints
  [conn queue task-id]
  (:n (jdbc/execute-one!
        conn [(str "select count(*) as n from absurd.c_" queue " where task_id = ?::uuid") task-id]
        row-opts)))

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
  (if (and (instance? PGobject v)
           (contains? #{"json" "jsonb"} (.getType ^PGobject v)))
    (some-> (.getValue ^PGobject v) (json/read-value mapper))
    v))

(defn- decode-row
  [row]
  (when row
    (update-vals row decode-value)))

(defn get-task
  "Task row from `absurd.t_<queue>` with jsonb columns decoded, or nil."
  [conn queue task-id]
  (decode-row
    (jdbc/execute-one!
      conn [(str "select * from absurd.t_" queue " where task_id = ?::uuid") task-id]
      row-opts)))

(defn get-run
  "Run row from `absurd.r_<queue>` with jsonb columns decoded, or nil."
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

(defn get-failure
  "Decoded failure payload of a run, or nil."
  [conn queue run-id]
  (:failure-reason (get-run conn queue run-id)))

(defn sleep-ms
  [ms]
  (Thread/sleep (long ms)))
