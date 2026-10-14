(ns absurd.sqlite-test
  "SQLite-specific behaviour: client wiring, lazy schema, deployment shapes.
  Each test runs on its own temp-file database, so no Docker or `with-conn`
  fixture is involved."
  (:require [absurd.core :as absurd]
            [absurd.sqlite :as sqlite]
            [absurd.store :as store]
            [absurd.test-helpers :as h]
            [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.io File]
           [java.sql Connection SQLException]
           [java.time Instant]
           [javax.sql DataSource]))

(defn- url-of
  [^File file]
  (str "jdbc:sqlite:" (.getAbsolutePath file)))

(defn- table-names
  [ds]
  (set (map :name (jdbc/execute! ds ["select name from sqlite_master where type = 'table'"]
                                 {:builder-fn rs/as-unqualified-kebab-maps}))))

(defn- run-one-task!
  "Register an echo task on `app`, spawn it, work it and return its result."
  [app]
  (absurd/create-queue! app)
  (absurd/register-task! app "echo" (fn [params _ctx] {:echo params}))
  (let [spawned (absurd/spawn! app "echo" {:value 1})]
    (absurd/work-batch! app {:worker-id "w"})
    (absurd/fetch-task-result app (:task-id spawned))))

(deftest a-sqlite-url-infers-the-backend-and-runs-a-task-end-to-end
  (h/call-with-sqlite-db
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
  (h/call-with-sqlite-db
    (fn [ds _file]
      (let [app (absurd/client {:db ds
                                :backend :sqlite})]
        (is (false? (:owned-db? app)))
        (is (= {:state :completed
                :result {:echo {:value 1}}}
               (run-one-task! app)))))))

(deftest a-single-shared-connection-works-from-several-threads
  (h/call-with-sqlite-db
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

(deftest in-memory-urls-are-rejected-but-a-memory-connection-works
  (doseq [url ["jdbc:sqlite::memory:" "jdbc:sqlite:" "jdbc:sqlite:file::memory:?cache=private"
               "jdbc:sqlite:file:x?mode=memory"]]
    (testing url
      (is (= :absurd/invalid-db (h/ex-type #(absurd/client {:db url}))))))
  (testing "a file URL with a query string is still accepted"
    (is (= :sqlite (:backend (absurd/client {:db "jdbc:sqlite:/tmp/absurd-x.db?busy_timeout=5000"})))))
  (testing "one shared Connection to a memory database is the supported way"
    (with-open [^Connection conn (jdbc/get-connection "jdbc:sqlite::memory:")]
      (is (= {:state :completed
              :result {:echo {:value 1}}}
             (run-one-task! (absurd/client {:db conn
                                            :backend :sqlite})))))))

(deftest the-engine-clock-is-read-inside-the-transaction
  (h/call-with-sqlite-db
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
  (h/call-with-sqlite-db
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
  (testing "SQLite takes a jdbc:sqlite: URL, a DataSource or a Connection"
    (is (= :absurd/invalid-db (h/ex-type #(absurd/client {:db "postgresql://localhost/x"
                                                          :backend :sqlite}))))
    (is (= :absurd/invalid-db (h/ex-type #(absurd/client {:db 42
                                                          :backend :sqlite})))))
  (testing "the queue is validated first, before anything else"
    (is (= :absurd/invalid-queue-name (h/ex-type #(absurd/client {:backend :mysql
                                                                  :queue ""}))))))

(defn- counting-datasource
  "A DataSource that counts `getConnection` calls and fails the first
  `failures` of them."
  [^DataSource delegate calls failures]
  (reify DataSource
    (getConnection [_]
      (when (<= (swap! calls inc) failures)
        (throw (SQLException. "database is unavailable")))
      (.getConnection delegate))))

(deftest the-constructor-never-connects-and-the-schema-is-created-lazily
  (h/call-with-sqlite-db
    (fn [ds _file]
      (let [calls (atom 0)
            app (absurd/client {:db (counting-datasource ds calls 0)
                                :backend :sqlite})]
        (is (zero? @calls) "the constructor does not touch the database")
        (is (empty? (table-names ds)))
        (is (= [] (absurd/list-queues app)))
        (is (pos? @calls))
        (is (contains? (table-names ds) "absurd_tasks") "the first operation created the schema")
        (let [before @calls]
          (absurd/list-queues app)
          (is (= (inc before) @calls) "later operations borrow one connection and skip the schema"))))))

(deftest a-failed-first-operation-is-retried
  (h/call-with-sqlite-db
    (fn [ds _file]
      (let [calls (atom 0)
            app (absurd/client {:db (counting-datasource ds calls 1)
                                :backend :sqlite})]
        (is (thrown-with-msg? SQLException #"database is unavailable" (absurd/list-queues app)))
        (is (= [] (absurd/list-queues app)) "the store recovers once the database is back")
        (is (contains? (table-names ds) "absurd_tasks"))))))

(deftest ensure-schema-can-run-at-boot-and-is-repeatable
  (h/call-with-sqlite-db
    (fn [ds _file]
      (sqlite/ensure-schema! ds)
      (sqlite/ensure-schema! ds)
      (is (contains? (table-names ds) "absurd_runs"))
      (let [app (absurd/client {:db ds
                                :backend :sqlite})]
        (is (= [] (absurd/list-queues app)))))))

(deftest tables-of-the-application-in-the-same-file-are-untouched
  (h/call-with-sqlite-db
    (fn [ds _file]
      (jdbc/execute! ds ["create table app_items (id integer primary key, name text)"])
      (jdbc/execute! ds ["insert into app_items (name) values ('keep me')"])
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
  (h/call-with-sqlite-db
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
