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

;; Queues, spawn and task results

(def ^:private t0
  "A fixed engine clock for the tests, in epoch milliseconds."
  1700000000000)

(defn- tx
  "Run `(f conn)` in a write transaction on the test database."
  [f]
  (engine/run-tx *env* :write f))

(defn- setup!
  "Create the schema and the queue `q`."
  []
  (schema/ensure-schema! *env*)
  (tx #(engine/create-queue % t0 "q" nil)))

(defn- exception-of
  [f]
  (try
    (f)
    nil
    (catch SQLException e e)))

(deftest create-queue-is-idempotent-and-rejects-partitioned
  (schema/ensure-schema! *env*)
  (tx #(engine/create-queue % t0 "q" nil))
  (tx #(engine/create-queue % t0 "q" "unpartitioned"))
  (is (= ["q"] (tx #(engine/list-queues % t0))))
  (testing "created_at is the engine clock"
    (is (= [{:created-at t0}] (query "select created_at from absurd_queues"))))
  (testing "partitioned queues are rejected before anything is stored"
    (let [e (exception-of #(tx (fn [c] (engine/create-queue c t0 "p" "partitioned"))))]
      (is (= "Partitioned queues are not supported on SQLite" (ex-message e)))
      (is (= ["q"] (tx #(engine/list-queues % t0))))))
  (testing "validation mirrors the Postgres function"
    (is (= "Queue name must be provided"
           (ex-message (exception-of #(tx (fn [c] (engine/create-queue c t0 "" nil)))))))
    (is (str/includes? (ex-message (exception-of #(tx (fn [c] (engine/create-queue c t0 (apply str (repeat 58 "q")) nil)))))
                       "is too long (max 57 bytes)"))
    (is (= "Unsupported queue storage mode \"weird\""
           (ex-message (exception-of #(tx (fn [c] (engine/create-queue c t0 "w" "weird"))))))))
  (testing "storage mode is trimmed and case-insensitive"
    (tx #(engine/create-queue % t0 "q" " Unpartitioned "))
    (is (= ["q"] (tx #(engine/list-queues % t0))))))

(deftest list-queues-is-ordered-by-name
  (schema/ensure-schema! *env*)
  (doseq [q ["b" "c" "a"]]
    (tx #(engine/create-queue % t0 q nil)))
  (is (= ["a" "b" "c"] (tx #(engine/list-queues % t0)))))

(deftest drop-queue-removes-its-rows-and-leaves-other-queues-alone
  (setup!)
  (tx #(engine/create-queue % t0 "other" nil))
  (doseq [q ["q" "other"]]
    (tx #(engine/spawn-task % t0 q "t" {:n 1} nil))
    (jdbc/execute! *ds* ["insert into absurd_checkpoints (queue_name, task_id, checkpoint_name, updated_at) values (?, 't', 's', 0)" q])
    (jdbc/execute! *ds* ["insert into absurd_events (queue_name, event_name, payload, emitted_at) values (?, 'e', null, 0)" q])
    (jdbc/execute! *ds* ["insert into absurd_waits (queue_name, task_id, run_id, step_name, event_name, created_at) values (?, 't', 'r', 's', 'e', 0)" q]))
  (tx #(engine/drop-queue % t0 "q"))
  (is (= ["other"] (tx #(engine/list-queues % t0))))
  (doseq [table ["absurd_tasks" "absurd_runs" "absurd_checkpoints" "absurd_events" "absurd_waits"]]
    (is (= [{:queue-name "other"}]
           (query (str "select distinct queue_name from " table)))
        table))
  (testing "dropping a missing queue is a no-op"
    (tx #(engine/drop-queue % t0 "missing"))
    (is (= ["other"] (tx #(engine/list-queues % t0))))))

(deftest queue-policy-defaults-and-verbatim-intervals
  (setup!)
  (is (= {:queue-name "q"
          :storage-mode "unpartitioned"
          :partition-lookahead "28 days"
          :partition-lookback "1 day"
          :cleanup-ttl "30 days"
          :cleanup-limit 1000
          :detach-mode "none"
          :detach-min-age "30 days"}
         (tx #(engine/get-queue-policy % t0 "q"))))
  (is (nil? (tx #(engine/get-queue-policy % t0 "missing"))))
  (tx #(engine/set-queue-policy! % t0 "q" {"cleanup_ttl" "12345 seconds"
                                           "cleanup_limit" 7
                                           "detach_mode" " EMPTY "
                                           "partition_lookahead" "2 Weeks"}))
  (let [policy (tx #(engine/get-queue-policy % t0 "q"))]
    (is (= "12345 seconds" (:cleanup-ttl policy)) "stored as given, not normalized")
    (is (= "2 Weeks" (:partition-lookahead policy)))
    (is (= 7 (:cleanup-limit policy)))
    (is (= "empty" (:detach-mode policy))))
  (testing "an empty policy is a no-op"
    (tx #(engine/set-queue-policy! % t0 "q" {}))
    (tx #(engine/set-queue-policy! % t0 "q" nil))
    (is (= 7 (:cleanup-limit (tx #(engine/get-queue-policy % t0 "q")))))))

(deftest queue-policy-validation
  (setup!)
  (doseq [[policy message]
          [[{"nope" 1} "Unsupported queue policy key \"nope\""]
           [{"cleanup_limit" 0} "cleanup_limit must be at least 1"]
           [{"cleanup_limit" "x"} "invalid input syntax for type integer: \"x\""]
           [{"detach_mode" "sometimes"} "Unsupported detach mode \"sometimes\""]
           [{"cleanup_ttl" "-1 days"} "Invalid interval value \"-1 days\" for cleanup_ttl"]
           [{"detach_min_age" "soon"} "Invalid interval value \"soon\" for detach_min_age"]
           [{"default_partition" "maybe"} "Unsupported default_partition mode \"maybe\""]
           [{"default_partition" "enabled"} "default_partition policy is only supported for partitioned queues"]
           [[1 2] "Queue policy must be a JSON object"]]]
    (testing (pr-str policy)
      (let [e (exception-of #(tx (fn [c] (engine/set-queue-policy! c t0 "q" policy))))]
        (is (= message (ex-message e))))))
  (is (= "Queue \"missing\" does not exist"
         (ex-message (exception-of #(tx (fn [c] (engine/set-queue-policy! c t0 "missing" {"cleanup_limit" 5}))))))
      "the queue must exist")
  (testing "a rejected policy changes nothing"
    (is (thrown? SQLException (tx #(engine/set-queue-policy! % t0 "q" {"cleanup_limit" 5
                                                                       "detach_mode" "sometimes"}))))
    (is (= 1000 (:cleanup-limit (tx #(engine/get-queue-policy % t0 "q")))))))

(deftest spawn-task-creates-a-pending-task-and-its-first-run
  (setup!)
  (let [spawned (tx #(engine/spawn-task % t0 "q" "echo" {:value 1}
                                        {"headers" {:trace-id "t1"}
                                         "max_attempts" 3
                                         "retry_strategy" {:kind "fixed"}
                                         "cancellation" {:max_delay 60}}))
        task (first (query "select * from absurd_tasks where task_id = ?" (:task-id spawned)))
        run (first (query "select * from absurd_runs where run_id = ?" (:run-id spawned)))]
    (is (= 1 (:attempt spawned)))
    (is (true? (:created spawned)))
    (is (not= (:task-id spawned) (:run-id spawned)))
    (is (= {:queue-name "q"
            :task-name "echo"
            :params "{\"value\":1}"
            :headers "{\"trace-id\":\"t1\"}"
            :retry-strategy "{\"kind\":\"fixed\"}"
            :max-attempts 3
            :cancellation "{\"max_delay\":60}"
            :enqueue-at t0
            :first-started-at nil
            :state "pending"
            :attempts 1
            :last-attempt-run (:run-id spawned)
            :completed-payload nil
            :cancelled-at nil
            :idempotency-key nil}
           (dissoc task :task-id)))
    (is (= {:queue-name "q"
            :task-id (:task-id spawned)
            :attempt 1
            :state "pending"
            :claimed-by nil
            :claim-expires-at nil
            :available-at t0
            :wake-event nil
            :event-payload nil
            :started-at nil
            :completed-at nil
            :failed-at nil
            :result nil
            :failure-reason nil
            :created-at t0}
           (dissoc run :run-id)))))

(deftest spawn-task-stores-nil-params-as-json-null-and-absent-options-as-sql-null
  (setup!)
  (let [{:keys [task-id]} (tx #(engine/spawn-task % t0 "q" "t" nil nil))
        task (first (query "select * from absurd_tasks where task_id = ?" task-id))]
    (is (= "null" (:params task)))
    (is (nil? (:headers task)))
    (is (nil? (:retry-strategy task)))
    (is (nil? (:max-attempts task)))
    (is (nil? (:cancellation task))))
  (testing "options given as keywords behave like string keys"
    (let [{:keys [task-id]} (tx #(engine/spawn-task % t0 "q" "t" {} {:max_attempts 2
                                                                     :headers {:a 1}}))
          task (first (query "select max_attempts, headers from absurd_tasks where task_id = ?" task-id))]
      (is (= {:max-attempts 2
              :headers "{\"a\":1}"}
             task)))))

(deftest spawn-task-with-an-idempotency-key-returns-the-existing-task
  (setup!)
  (let [first-spawn (tx #(engine/spawn-task % t0 "q" "t" {:n 1} {"idempotency_key" "k1"}))
        again (tx #(engine/spawn-task % (inc t0) "q" "t" {:n 2} {"idempotency_key" "k1"}))
        other (tx #(engine/spawn-task % t0 "q" "t" {:n 3} {"idempotency_key" "k2"}))]
    (is (true? (:created first-spawn)))
    (is (false? (:created again)))
    (is (= (:task-id first-spawn) (:task-id again)))
    (is (= (:run-id first-spawn) (:run-id again)))
    (is (= 1 (:attempt again)))
    (is (true? (:created other)))
    (is (= 2 (:n (first (query "select count(*) as n from absurd_tasks")))))
    (is (= 2 (:n (first (query "select count(*) as n from absurd_runs"))))))
  (testing "the key is scoped to the queue"
    (tx #(engine/create-queue % t0 "other" nil))
    (is (true? (:created (tx #(engine/spawn-task % t0 "other" "t" nil {"idempotency_key" "k1"})))))))

(deftest spawn-task-validates-its-input
  (setup!)
  (doseq [[task-name options message]
          [[nil nil "task_name must be provided"]
           ["  " nil "task_name must be provided"]
           ["t" {"max_attempts" 0} "max_attempts must be >= 1"]
           ["t" {"retry_strategy" {:kind "linear"}} "Unsupported retry strategy kind \"linear\""]]]
    (testing (pr-str [task-name options])
      (is (= message (ex-message (exception-of #(tx (fn [c] (engine/spawn-task c t0 "q" task-name nil options)))))))))
  (is (= "AB003" (.getSQLState ^SQLException (exception-of #(tx (fn [c] (engine/spawn-task c t0 "q" "t" nil {"retry_strategy" {:kind "linear"}})))))))
  (is (zero? (:n (first (query "select count(*) as n from absurd_tasks")))) "rejected spawns store nothing")
  (testing "a missing queue is an error, as on Postgres"
    (let [e (exception-of #(tx (fn [c] (engine/spawn-task c t0 "missing" "t" nil nil))))]
      (is (= "Queue \"missing\" does not exist" (ex-message e)))
      (is (= "42P01" (.getSQLState e))))))

(deftest get-task-result-projects-state-result-and-failure-reason
  (setup!)
  (is (nil? (tx #(engine/get-task-result % t0 "q" (str (random-uuid))))))
  (let [{:keys [task-id run-id]} (tx #(engine/spawn-task % t0 "q" "t" nil nil))]
    (is (= {:task-id task-id
            :state "pending"
            :result nil
            :failure-reason nil}
           (tx #(engine/get-task-result % t0 "q" task-id))))
    (jdbc/execute! *ds* ["update absurd_tasks set state = 'completed', completed_payload = '{\"ok\":true}' where task_id = ?" task-id])
    (is (= {:task-id task-id
            :state "completed"
            :result {:ok true}
            :failure-reason nil}
           (tx #(engine/get-task-result % t0 "q" task-id))))
    (jdbc/execute! *ds* ["update absurd_runs set failure_reason = '{\"message\":\"boom\"}' where run_id = ?" run-id])
    (jdbc/execute! *ds* ["update absurd_tasks set state = 'failed' where task_id = ?" task-id])
    (is (= {:task-id task-id
            :state "failed"
            :result nil
            :failure-reason {:message "boom"}}
           (tx #(engine/get-task-result % t0 "q" task-id))))
    (testing "a result is only exposed for completed tasks, and a failure only for failed ones"
      (jdbc/execute! *ds* ["update absurd_tasks set state = 'running' where task_id = ?" task-id])
      (is (= {:task-id task-id
              :state "running"
              :result nil
              :failure-reason nil}
             (tx #(engine/get-task-result % t0 "q" task-id)))))))
