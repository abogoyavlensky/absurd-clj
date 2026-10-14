(ns absurd.sqlite.engine-test
  "Focused tests for the SQLite engine. They run against temp-file databases
  and need neither Docker nor the `with-conn` fixture."
  (:require [absurd.sqlite.engine :as engine]
            [absurd.sqlite.schema :as schema]
            [absurd.test-helpers :as h]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.sql SQLException]
           [java.util.concurrent.locks ReentrantLock]))

(def ^:dynamic *ds* nil)
(def ^:dynamic *env* nil)

(use-fixtures :each
  (fn [f]
    (h/call-with-sqlite-db
      (fn [ds _file]
        (binding [*ds* ds
                  *env* {:connectable ds
                         :lock (ReentrantLock.)}]
          (f))))))

(defn- query
  [sql & params]
  (jdbc/execute! *ds* (into [sql] params) {:builder-fn rs/as-unqualified-kebab-maps}))

(defn- sqlstate-of
  [f]
  (try
    (f)
    nil
    (catch SQLException e
      (.getSQLState e))))

;; Schema

(deftest ensure-schema-is-idempotent-and-creates-every-table
  (schema/ensure-schema! *env*)
  (schema/ensure-schema! *env*)
  (is (= #{"absurd_meta" "absurd_queues" "absurd_tasks" "absurd_runs"
           "absurd_checkpoints" "absurd_events" "absurd_waits"}
         (set (map :name (query "select name from sqlite_master
                                  where type = 'table' and name like 'absurd_%'")))))
  (is (= [{:value schema/schema-version}]
         (query "select value from absurd_meta where key = 'schema_version'"))))

(deftest ensure-schema-rejects-a-different-version
  (schema/ensure-schema! *env*)
  (jdbc/execute! *ds* ["update absurd_meta set value = '99' where key = 'schema_version'"])
  (let [e (try (schema/ensure-schema! *env*)
               nil
               (catch clojure.lang.ExceptionInfo e e))]
    (is (= :absurd/schema-version-mismatch (:type (ex-data e))))
    (is (str/includes? (ex-message e) "99"))))

(deftest ensure-schema-enables-wal
  (schema/ensure-schema! *env*)
  (is (= [{:journal-mode "wal"}] (query "pragma journal_mode"))))

;; Transactions

(deftest with-tx-commits-on-success
  (schema/ensure-schema! *env*)
  (is (= :done (engine/with-tx [conn *env*]
                 (jdbc/execute! conn ["insert into absurd_meta (key, value) values ('k', 'v')"])
                 :done)))
  (is (= [{:value "v"}] (query "select value from absurd_meta where key = 'k'"))))

(deftest with-tx-rolls-back-when-the-body-throws
  (schema/ensure-schema! *env*)
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"boom"
                        (engine/with-tx [conn *env*]
                          (jdbc/execute! conn ["insert into absurd_meta (key, value) values ('k', 'v')"])
                          (throw (ex-info "boom" {})))))
  (is (empty? (query "select value from absurd_meta where key = 'k'")))
  (testing "the lock is released and the next transaction works"
    (is (= 1 (engine/with-tx [_ *env*] 1)))))

(deftest with-read-tx-runs-a-deferred-transaction
  (schema/ensure-schema! *env*)
  (is (= [{:n 1}] (engine/with-read-tx [conn *env*]
                    (jdbc/execute! conn ["select count(*) as n from absurd_meta"]
                                   {:builder-fn rs/as-unqualified-kebab-maps})))))

(deftest with-tx-rejects-a-connection-with-autocommit-off
  (with-open [conn (jdbc/get-connection *ds*)]
    (.setAutoCommit conn false)
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"autocommit"
                          (engine/with-tx [_ {:connectable conn
                                              :lock (ReentrantLock.)}]
                            :unreachable)))))

(deftest with-tx-uses-a-given-connection-without-closing-it
  (schema/ensure-schema! *env*)
  (with-open [conn (jdbc/get-connection *ds*)]
    (let [env {:connectable conn
               :lock (ReentrantLock.)}]
      (engine/with-tx [c env] (jdbc/execute! c ["insert into absurd_meta (key, value) values ('a', '1')"]))
      (engine/with-tx [c env] (jdbc/execute! c ["insert into absurd_meta (key, value) values ('b', '2')"]))
      (is (false? (.isClosed conn))))))

;; Helpers

(deftest times-convert-between-instants-and-epoch-millis
  (let [i (java.time.Instant/parse "2024-05-05T10:00:00.123Z")]
    (is (= 1714903200123 (engine/instant->ms i)))
    (is (= i (engine/ms->instant 1714903200123))))
  (is (= Long/MAX_VALUE engine/infinity)))

(deftest json-columns-encode-nil-as-the-json-text-null
  (is (= "null" (engine/encode-json nil)) "never SQL NULL: pass nil to a statement for that")
  (is (= "{\"a\":[1,2]}" (engine/encode-json {:a [1 2]})))
  (is (nil? (engine/decode-json nil)))
  (is (nil? (engine/decode-json "null")))
  (is (= {:a [1 2]} (engine/decode-json "{\"a\":[1,2]}"))))

(deftest uuidv7-has-version-variant-and-time-order
  (let [ids (mapv engine/uuidv7 (range 1700000000000 1700000000050))]
    (is (every? #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}" %) ids))
    (is (= ids (sort ids)) "ids generated in later milliseconds sort later"))
  (testing "ids generated for the same millisecond still increase"
    (let [ids (vec (repeatedly 200 #(engine/uuidv7 1800000000000)))]
      (is (= ids (sort ids)))
      (is (= 200 (count (set ids)))))))

;; retry_delay_seconds

(deftest retry-delay-seconds-matches-postgres
  (testing "no strategy and kind none"
    (is (= 0.0 (engine/retry-delay-seconds nil 1)))
    (is (= 0.0 (engine/retry-delay-seconds {} 3)))
    (is (= 0.0 (engine/retry-delay-seconds {:kind "none"} 3))))
  (testing "fixed"
    (is (= 60.0 (engine/retry-delay-seconds {:kind "fixed"} 1)))
    (is (= 60.0 (engine/retry-delay-seconds {:kind "fixed"} 5)))
    (is (= 7.5 (engine/retry-delay-seconds {:kind "fixed"
                                            :base_seconds 7.5} 2))))
  (testing "exponential defaults to base 30, factor 2 and the one day cap"
    (is (= 30.0 (engine/retry-delay-seconds {:kind "exponential"} 1)))
    (is (= 60.0 (engine/retry-delay-seconds {:kind "exponential"} 2)))
    (is (= 240.0 (engine/retry-delay-seconds {:kind "exponential"} 4)))
    (is (= 86400.0 (engine/retry-delay-seconds {:kind "exponential"} 20)))
    (is (= 90.0 (engine/retry-delay-seconds {:kind "exponential"
                                             :base_seconds 10
                                             :factor 3
                                             :max_seconds 100} 3)))
    (is (= 100.0 (engine/retry-delay-seconds {:kind "exponential"
                                              :base_seconds 10
                                              :factor 3
                                              :max_seconds 100} 4)))
    (is (= 0.0 (engine/retry-delay-seconds {:kind "exponential"
                                            :base_seconds 0} 5))))
  (testing "overflow saturates at the cap, or at zero for a factor below one"
    (is (= 86400.0 (engine/retry-delay-seconds {:kind "exponential"
                                                :factor 10} 1000)))
    (is (= 50.0 (engine/retry-delay-seconds {:kind "exponential"
                                             :factor 10
                                             :max_seconds 50} 1000)))
    (is (= 0.0 (engine/retry-delay-seconds {:kind "exponential"
                                            :factor 0.5} 5000))))
  (testing "a missing attempt counts as the first one"
    (is (= 30.0 (engine/retry-delay-seconds {:kind "exponential"} nil))))
  (testing "zero is valid even when written with an exponent"
    (is (= 0.0 (engine/retry-delay-seconds {:kind "fixed"
                                            :base_seconds "0e-999"} 1)))))

(deftest retry-delay-seconds-rejects-invalid-input
  (doseq [[strategy message] [["not-an-object" "retry_strategy must be a JSON object"]
                              [[1 2] "retry_strategy must be a JSON object"]
                              [{:kind "linear"} "Unsupported retry strategy kind \"linear\""]
                              [{:kind "fixed"
                                :base_seconds -1} "retry_strategy.base_seconds must be between 0 and 86400"]
                              [{:kind "fixed"
                                :base_seconds 86401} "retry_strategy.base_seconds must be between 0 and 86400"]
                              [{:kind "exponential"
                                :factor -1} "retry_strategy.factor must be a finite non-negative number"]
                              [{:kind "exponential"
                                :factor "Infinity"} "retry_strategy.factor must be a finite non-negative number"]
                              [{:kind "exponential"
                                :max_seconds 86401} "retry_strategy.max_seconds must be between 0 and 86400"]
                              [{:kind "fixed"
                                :base_seconds "abc"} "retry_strategy contains an invalid number"]
                              [{:kind "fixed"
                                :base_seconds "1e-999"} "retry_strategy contains an invalid number"]
                              [{:kind "fixed"
                                :base_seconds "1e999"} "retry_strategy contains an invalid number"]]]
    (testing (pr-str strategy)
      (let [e (try (engine/retry-delay-seconds strategy 1)
                   nil
                   (catch SQLException e e))]
        (is (some? e))
        (is (= "AB003" (some-> e .getSQLState)))
        (is (= message (ex-message e))))))
  (is (= "AB003" (sqlstate-of #(engine/retry-delay-seconds "x" 1)))))
