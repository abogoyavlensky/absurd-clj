(ns absurd.sqlite-test
  "SQLite-specific behaviour: client wiring, explicit schema, deployment shapes.
  Each test runs on its own temp-file database, so no Docker or `with-conn`
  fixture is involved."
  (:require [absurd.core :as absurd]
            [absurd.sqlite :as sqlite]
            [absurd.store :as store]
            [absurd.test-helpers :as h]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [ragtime.core :as ragtime]
            [ragtime.next-jdbc :as ragtime-jdbc]
            [ragtime.protocols :as ragtime-protocols])
  (:import [java.io File]
           [java.sql Connection SQLException]
           [java.time Instant]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicLong]
           [javax.sql DataSource]))

(defn- url-of
  [^File file]
  (str "jdbc:sqlite:" (.getAbsolutePath file)))

(defn- table-names
  [ds]
  (set (map :name (jdbc/execute! ds ["select name from sqlite_master where type = 'table'"]
                                 {:builder-fn rs/as-unqualified-kebab-maps}))))

(defn- call-with-schema-db
  "Like `h/call-with-sqlite-db`, with the Absurd schema already created."
  [f]
  (h/call-with-sqlite-db
    (fn [ds file]
      (sqlite/ensure-schema! ds)
      (f ds file))))

(defn- run-one-task!
  "Register an echo task on `app`, spawn it, work it and return its result."
  [app]
  (absurd/create-queue! app)
  (absurd/register-task! app "echo" (fn [params _ctx] {:echo params}))
  (let [spawned (absurd/spawn! app "echo" {:value 1})]
    (absurd/work-batch! app {:worker-id "w"})
    (absurd/fetch-task-result app (:task-id spawned))))

(deftest a-sqlite-url-infers-the-backend-and-runs-a-task-end-to-end
  (call-with-schema-db
    (fn [_ds file]
      (let [app (absurd/client {:db (url-of file)})]
        (is (= :sqlite (:backend app)))
        (is (true? (:owned-db? app)))
        (is (instance? absurd.sqlite.SqliteStore (:store app)))
        (is (= {:state :completed
                :result {:echo {:value 1}}}
               (run-one-task! app)))
        (absurd/close! app)))))

(deftest a-datasource-with-the-sqlite-backend-works
  (call-with-schema-db
    (fn [ds _file]
      (let [app (absurd/client {:db ds
                                :backend :sqlite})]
        (is (false? (:owned-db? app)))
        (is (= {:state :completed
                :result {:echo {:value 1}}}
               (run-one-task! app)))))))

(deftest a-single-shared-connection-works-from-several-threads
  (call-with-schema-db
    (fn [ds _file]
      (with-open [^Connection conn (jdbc/get-connection ds)]
        (let [app (absurd/client {:db conn
                                  :backend :sqlite})]
          (is (= {:state :completed
                  :result {:echo {:value 1}}}
                 (run-one-task! app)))
          (let [spawned (doall (pmap (fn [i] (absurd/spawn! app "echo" {:value i})) (range 20)))]
            (is (= 20 (count (set (map :task-id spawned))))))
          (is (false? (.isClosed conn)) "the client never closes a connection it was given"))))))

(deftest clients-sharing-one-connection-take-turns
  (call-with-schema-db
    (fn [ds _file]
      (with-open [^Connection conn (jdbc/get-connection ds)]
        (let [apps (mapv #(absurd/client {:db conn
                                          :backend :sqlite
                                          :queue %})
                         ["q1" "q2"])
              spawn-all (fn [app]
                          (future
                            (dotimes [i 100]
                              (absurd/spawn! app "t" {:i i} {:queue (:queue app)}))))]
          (run! absurd/create-queue! apps)
          ;; deref rethrows a failed spawn, e.g. a transaction started inside another
          (run! deref (mapv spawn-all apps))
          (is (= [{:queue-name "q1"
                   :n 100}
                  {:queue-name "q2"
                   :n 100}]
                 (jdbc/execute! conn ["select queue_name, count(*) as n from absurd_tasks
                                        group by queue_name order by queue_name"]
                                {:builder-fn rs/as-unqualified-kebab-maps}))))))))

(deftest in-memory-urls-are-rejected-but-a-memory-connection-works
  (doseq [url ["jdbc:sqlite::memory:" "jdbc:sqlite:" "jdbc:sqlite:file::memory:?cache=private"
               "jdbc:sqlite:file:x?mode=memory"]]
    (testing url
      (is (= :absurd/invalid-db (h/ex-type #(absurd/client {:db url}))))))
  (testing "a file URL with a query string is still accepted"
    (is (= :sqlite (:backend (absurd/client {:db "jdbc:sqlite:/tmp/absurd-x.db?busy_timeout=5000"})))))
  (testing "one shared Connection to a memory database is the supported way"
    (with-open [^Connection conn (jdbc/get-connection "jdbc:sqlite::memory:")]
      (sqlite/ensure-schema! conn)
      (is (= {:state :completed
              :result {:echo {:value 1}}}
             (run-one-task! (absurd/client {:db conn
                                            :backend :sqlite})))))))

(deftest the-engine-clock-is-read-inside-the-transaction
  (call-with-schema-db
    (fn [ds _file]
      (let [holder (atom nil)
            held? (atom [])
            st (sqlite/store ds {:db-now-fn (fn []
                                              (swap! held? conj (.isHeldByCurrentThread
                                                                  ^java.util.concurrent.locks.ReentrantLock
                                                                  (:lock @holder)))
                                              (Instant/now))})]
        (reset! holder st)
        (store/create-queue! st "q" nil)
        (store/list-queues st)
        (store/current-time st)
        (is (= [true true false] @held?)
            "operations read the clock under the lock; current-time is not an operation")))))

(deftest the-engine-clock-is-pluggable
  (call-with-schema-db
    (fn [ds _file]
      (let [pinned (Instant/parse "2030-01-01T00:00:00.123456Z")
            app (absurd/client {:db ds
                                :backend :sqlite
                                :db-now-fn (constantly pinned)})]
        (is (= (Instant/parse "2030-01-01T00:00:00.123Z") (store/current-time (:store app)))
            "truncated to milliseconds")
        (absurd/create-queue! app)
        (let [spawned (absurd/spawn! app "t" {} {:queue (:queue app)})
              [claim] (absurd/claim-tasks! app {:worker-id "w"
                                                :claim-timeout 60})]
          (is (= (:run-id spawned) (:run-id claim)))
          (is (= [{:claim-expires-at (+ (.toEpochMilli pinned) 60000)}]
                 (jdbc/execute! ds ["select claim_expires_at from absurd_runs"]
                                {:builder-fn rs/as-unqualified-kebab-maps}))))))))

(deftest backend-and-db-are-validated
  (testing "SQLite needs :db, there is no environment fallback"
    (is (= :absurd/invalid-db (h/ex-type #(absurd/client {:backend :sqlite})))))
  (testing "an unknown backend"
    (is (= :absurd/invalid-backend (h/ex-type #(absurd/client {:db "jdbc:sqlite:x.db"
                                                               :backend :mysql}))))
    (is (= :absurd/invalid-backend (h/ex-type #(absurd/client {:db "jdbc:sqlite:x.db"
                                                               :backend "sqlite"})))))
  (testing "a SQLite URL cannot be forced onto the Postgres backend"
    (is (= :absurd/invalid-db (h/ex-type #(absurd/client {:db "jdbc:sqlite:x.db"
                                                          :backend :postgres})))))
  (testing "SQLite takes a jdbc:sqlite: URL, a DataSource or a Connection"
    (is (= :absurd/invalid-db (h/ex-type #(absurd/client {:db "postgresql://localhost/x"
                                                          :backend :sqlite}))))
    (is (= :absurd/invalid-db (h/ex-type #(absurd/client {:db 42
                                                          :backend :sqlite})))))
  (testing "the queue is validated first, before anything else"
    (is (= :absurd/invalid-queue-name (h/ex-type #(absurd/client {:backend :mysql
                                                                  :queue ""}))))))

(defn- counting-datasource
  "A DataSource that counts `getConnection` calls."
  [^DataSource delegate calls]
  (reify DataSource
    (getConnection [_]
      (swap! calls inc)
      (.getConnection delegate))))

(deftest operations-need-an-explicit-schema
  (h/call-with-sqlite-db
    (fn [ds _file]
      (let [calls (atom 0)
            app (absurd/client {:db (counting-datasource ds calls)
                                :backend :sqlite})]
        (is (zero? @calls) "the constructor does not touch the database")
        (is (= :absurd/schema-missing (h/ex-type #(absurd/list-queues app))))
        (absurd/register-task! app "t" (fn [_params _ctx] nil))
        (is (= :absurd/schema-missing (h/ex-type #(absurd/spawn! app "t" {}))))
        (is (instance? SQLException (ex-cause (try (absurd/list-queues app) (catch Exception e e))))
            "the driver error is kept as the cause")
        (is (empty? (table-names ds)) "no operation created the schema")
        (sqlite/ensure-schema! ds)
        (is (= [] (absurd/list-queues app)))))))

(deftest ensure-schema-can-run-at-boot-and-is-repeatable
  (h/call-with-sqlite-db
    (fn [ds _file]
      (sqlite/ensure-schema! ds)
      (sqlite/ensure-schema! ds)
      (is (contains? (table-names ds) "absurd_runs"))
      (let [app (absurd/client {:db ds
                                :backend :sqlite})]
        (is (= [] (absurd/list-queues app)))))))

(deftest the-shipped-migration-applies-with-ragtime
  (h/call-with-sqlite-db
    (fn [_ds file]
      (let [ds (jdbc/get-datasource {:jdbcUrl (str (url-of file) "?journal_mode=WAL")})
            migrations (vec (ragtime-jdbc/load-resources "absurd/sqlite/migrations"))
            index (ragtime/into-index migrations)
            database (ragtime-jdbc/sql-database ds)]
        (is (= ["001-absurd-init"] (map ragtime-protocols/id migrations)))
        (ragtime/migrate-all database index migrations)
        (is (= {:state :completed
                :result {:echo {:value 1}}}
               (run-one-task! (absurd/client {:db ds
                                              :backend :sqlite}))))
        (is (= [{:journal-mode "wal"}]
               (jdbc/execute! ds ["pragma journal_mode"] {:builder-fn rs/as-unqualified-kebab-maps}))
            "the journal_mode URL parameter put the file in WAL")
        (ragtime/rollback-last database index)
        (is (empty? (filter #(str/starts-with? % "absurd_") (table-names ds))))))))

(deftest tables-of-the-application-in-the-same-file-are-untouched
  (h/call-with-sqlite-db
    (fn [ds _file]
      (jdbc/execute! ds ["create table app_items (id integer primary key, name text)"])
      (jdbc/execute! ds ["insert into app_items (name) values ('keep me')"])
      (sqlite/ensure-schema! ds)
      (let [app (absurd/client {:db ds
                                :backend :sqlite})]
        (is (= {:state :completed
                :result {:echo {:value 1}}}
               (run-one-task! app)))
        (absurd/drop-queue! app))
      (is (= #{"app_items" "absurd_meta" "absurd_queues" "absurd_tasks" "absurd_runs"
               "absurd_checkpoints" "absurd_events" "absurd_waits"}
             (disj (table-names ds) "sqlite_sequence")))
      (is (= [{:name "keep me"}]
             (jdbc/execute! ds ["select name from app_items"]
                            {:builder-fn rs/as-unqualified-kebab-maps}))))))

(deftest sqlite-errors-surface-like-the-postgres-ones
  (call-with-schema-db
    (fn [ds _file]
      (let [app (absurd/client {:db ds
                                :backend :sqlite})]
        (absurd/create-queue! app)
        (testing "AB001 becomes the cancelled signal"
          (let [spawned (absurd/spawn! app "t" {} {:queue (:queue app)})
                [claim] (absurd/claim-tasks! app {:worker-id "w"})]
            (absurd/cancel-task! app (:task-id spawned))
            (is (= :absurd/cancelled
                   (h/ex-type #(store/set-task-checkpoint-state! (:store app) (:queue app) (:task-id spawned)
                                                                 "s" 1 (:run-id claim) 60))))))
        (testing "AB002 becomes the failed-run signal"
          (let [spawned (absurd/spawn! app "t" {} {:queue (:queue app)
                                                   :max-attempts 1})
                [claim] (absurd/claim-tasks! app {:worker-id "w"})]
            (store/fail-run! (:store app) (:queue app) (:run-id claim) {:name "E"})
            (is (= :absurd/failed-run
                   (h/ex-type #(store/complete-run! (:store app) (:queue app) (:run-id spawned) nil))))))
        (testing "other engine errors stay SQLExceptions"
          (is (thrown-with-msg? SQLException #"not found"
                                (absurd/cancel-task! app (str (random-uuid))))))
        (testing "partitioned queues are rejected"
          (is (thrown-with-msg? SQLException #"Partitioned queues are not supported on SQLite"
                                (absurd/create-queue! app "p" {:storage-mode :partitioned}))))))))

;; Concurrency

(defn- wait-until
  "Poll `pred` for up to `seconds`; returns true when it became truthy."
  [pred seconds]
  (let [deadline (+ (System/nanoTime) (* seconds 1e9))]
    (loop []
      (cond
        (pred) true
        (> (System/nanoTime) deadline) false
        :else (do (Thread/sleep 50) (recur))))))

(defn- count-where
  [ds sql]
  (:n (jdbc/execute-one! ds [sql] {:builder-fn rs/as-unqualified-kebab-maps})))

(defn- spawn-concurrently!
  "Spawn `n` tasks named `task-name` from `threads` threads, each on its own
  client over the same database (so they contend for SQLite's write lock the
  way separate processes would). Returns the task ids."
  [ds queue task-name n threads]
  (let [producers (repeatedly threads #(absurd/client {:db ds
                                                       :backend :sqlite
                                                       :queue queue}))
        futures (doall (map-indexed
                         (fn [t producer]
                           (future
                             (mapv #(:task-id (absurd/spawn! producer task-name {:i %} {:queue queue}))
                                   (range t n threads))))
                         producers))]
    (into [] (mapcat deref) futures)))

(deftest concurrent-workers-complete-every-task-exactly-once
  (call-with-schema-db
    (fn [ds _file]
      (let [app (absurd/client {:db ds
                                :backend :sqlite
                                :queue "stress"})
            executions (AtomicLong. 0)
            seen (ConcurrentHashMap/newKeySet)]
        (absurd/create-queue! app)
        (absurd/register-task! app "count" (fn [params ctx]
                                             (.incrementAndGet executions)
                                             (.add seen (:task-id ctx))
                                             {:i (:i params)}))
        (with-open [worker (absurd/start-worker! app {:worker-id "stress"
                                                      :concurrency 4
                                                      :claim-timeout 60
                                                      :poll-interval 10})]
          (let [task-ids (spawn-concurrently! ds "stress" "count" 200 4)]
            (is (= 200 (count (set task-ids))))
            (is (wait-until #(= 200 (count-where ds "select count(*) as n from absurd_tasks where state = 'completed'"))
                            60))
            (is (true? (absurd/stop-worker! worker)))
            (is (= 200 (.get executions)) "every task body ran exactly once")
            (is (= (set task-ids) (set seen)))
            (is (zero? (count-where ds "select count(*) as n from absurd_tasks where state <> 'completed'")))
            (is (= 200 (count-where ds "select count(*) as n from absurd_runs"))
                "no task was claimed twice")
            (is (zero? (count-where ds "select count(*) as n from absurd_runs where state <> 'completed'")))))))))

(deftest steps-run-once-per-task-under-concurrency
  (call-with-schema-db
    (fn [ds _file]
      (let [app (absurd/client {:db ds
                                :backend :sqlite
                                :queue "steps"})
            a-runs (ConcurrentHashMap.)
            b-runs (ConcurrentHashMap.)
            bump! (fn [^ConcurrentHashMap m task-id]
                    (.merge m task-id 1 (fn [x y] (+ (long x) (long y)))))]
        (absurd/create-queue! app)
        (absurd/register-task! app "two-steps"
                               (fn [_params ctx]
                                 (absurd/step ctx "a" (fn [] (bump! a-runs (:task-id ctx)) :a))
                                 (absurd/step ctx "b" (fn [] (bump! b-runs (:task-id ctx)) :b))
                                 ;; Outside both steps, after both checkpoints committed: attempt 2
                                 ;; must replay them from the checkpoints instead of running them again.
                                 (when (= 1 (:attempt ctx))
                                   (throw (ex-info "retry me" {})))
                                 {:attempt (:attempt ctx)}))
        (with-open [worker (absurd/start-worker! app {:worker-id "steps"
                                                      :concurrency 4
                                                      :claim-timeout 60
                                                      :poll-interval 10})]
          (let [task-ids (spawn-concurrently! ds "steps" "two-steps" 200 4)]
            (is (wait-until #(= 200 (count-where ds "select count(*) as n from absurd_tasks where state = 'completed'"))
                            90))
            (is (true? (absurd/stop-worker! worker)))
            (is (= #{{:state :completed
                      :result {:attempt 2}}}
                   (set (map #(absurd/fetch-task-result app %) task-ids)))
                "every task completed on attempt 2")
            (is (= #{1} (set (vals a-runs))) "step a ran once per task")
            (is (= #{1} (set (vals b-runs))) "step b ran once per task")
            (is (= (set task-ids) (set (keys a-runs))))
            (is (= 400 (count-where ds "select count(*) as n from absurd_runs")) "two attempts per task")
            (is (= 400 (count-where ds "select count(*) as n from absurd_checkpoints")) "two checkpoints per task")))))))
