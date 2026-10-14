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
  (tx #(engine/set-queue-policy % t0 "q" {"cleanup_ttl" "12345 seconds"
                                          "cleanup_limit" 7
                                          "detach_mode" " EMPTY "
                                          "partition_lookahead" "2 Weeks"}))
  (let [policy (tx #(engine/get-queue-policy % t0 "q"))]
    (is (= "12345 seconds" (:cleanup-ttl policy)) "stored as given, not normalized")
    (is (= "2 Weeks" (:partition-lookahead policy)))
    (is (= 7 (:cleanup-limit policy)))
    (is (= "empty" (:detach-mode policy))))
  (testing "an empty policy is a no-op"
    (tx #(engine/set-queue-policy % t0 "q" {}))
    (tx #(engine/set-queue-policy % t0 "q" nil))
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
      (let [e (exception-of #(tx (fn [c] (engine/set-queue-policy c t0 "q" policy))))]
        (is (= message (ex-message e))))))
  (is (= "Queue \"missing\" does not exist"
         (ex-message (exception-of #(tx (fn [c] (engine/set-queue-policy c t0 "missing" {"cleanup_limit" 5}))))))
      "the queue must exist")
  (testing "a rejected policy changes nothing"
    (is (thrown? SQLException (tx #(engine/set-queue-policy % t0 "q" {"cleanup_limit" 5
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
           ["t" {"retry_strategy" {:kind "linear"}} "Unsupported retry strategy kind \"linear\""]
           ["t" {"retry_strategy" nil} "retry_strategy must be a JSON object"]]]
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

;; Claims, run lifecycle, cancel and retry

(defn- spawn!
  "Spawn a task on queue `q` at `now`; returns the spawn result."
  ([now] (spawn! now nil))
  ([now options] (spawn! now "t" options))
  ([now task-name options]
   (tx #(engine/spawn-task % now "q" task-name {:n 1} options))))

(defn- claim!
  ([now] (claim! now 1))
  ([now qty] (claim! now qty 60))
  ([now qty timeout] (tx #(engine/claim-tasks % now "q" "w1" timeout qty))))

(defn- task-row
  [task-id]
  (first (query "select * from absurd_tasks where task_id = ?" task-id)))

(defn- run-row
  [run-id]
  (first (query "select * from absurd_runs where run_id = ?" run-id)))

(defn- runs-of
  [task-id]
  (query "select * from absurd_runs where task_id = ? order by attempt" task-id))

(defn- sqlstate
  [f]
  (some-> (exception-of f) .getSQLState))

(defn- message
  [f]
  (some-> (exception-of f) ex-message))

(defn- delay-run!
  "Make a pending run not due until `at`."
  [run-id at]
  (jdbc/execute! *ds* ["update absurd_runs set available_at = ? where run_id = ?" at run-id]))

(deftest claim-tasks-honours-qty-and-availability-and-marks-runs-running
  (setup!)
  (let [a (spawn! t0)
        b (spawn! (+ t0 1))
        c (spawn! (+ t0 2))
        later (spawn! (+ t0 3))]
    (delay-run! (:run-id later) (+ t0 1000))
    (testing "only the first qty runs are claimed, oldest first"
      (let [claims (claim! (+ t0 10) 2 120)]
        (is (= [(:run-id a) (:run-id b)] (mapv :run-id claims)))
        (is (= {:run-id (:run-id a)
                :task-id (:task-id a)
                :attempt 1
                :task-name "t"
                :params {:n 1}
                :retry-strategy nil
                :max-attempts nil
                :headers nil
                :wake-event nil
                :event-payload nil}
               (first claims)))))
    (testing "claimed runs are running with a lease"
      (is (= {:state "running"
              :claimed-by "w1"
              :claim-expires-at (+ t0 10 120000)
              :started-at (+ t0 10)
              :available-at (+ t0 10)}
             (select-keys (run-row (:run-id a)) [:state :claimed-by :claim-expires-at :started-at :available-at])))
      (is (= {:state "running"
              :attempts 1
              :first-started-at (+ t0 10)
              :last-attempt-run (:run-id a)}
             (select-keys (task-row (:task-id a)) [:state :attempts :first-started-at :last-attempt-run]))))
    (testing "a run that is not due yet is left alone until available_at"
      (is (= [(:run-id c)] (mapv :run-id (claim! (+ t0 11) 5))))
      (is (empty? (claim! (+ t0 999) 5)))
      (is (= [(:run-id later)] (mapv :run-id (claim! (+ t0 1000) 5)))))
    (testing "nothing is left to claim"
      (is (empty? (claim! (+ t0 2000) 5))))
    (testing "a sleeping run waits until its wake time"
      (let [d (spawn! (+ t0 3000))]
        (claim! (+ t0 3000))
        (tx #(engine/schedule-run-after % (+ t0 3000) "q" (:run-id d) 30))
        (is (empty? (claim! (+ t0 3000 29999) 1)))
        (is (= [(:run-id d)] (mapv :run-id (claim! (+ t0 3000 30000) 1))))))))

(deftest claim-tasks-with-a-zero-timeout-has-no-lease-and-defaults-the-worker
  (setup!)
  (let [spawned (spawn! t0)]
    (is (= 1 (count (tx #(engine/claim-tasks % t0 "q" "" 0 0)))))
    (is (= {:claimed-by "worker"
            :claim-expires-at nil}
           (select-keys (run-row (:run-id spawned)) [:claimed-by :claim-expires-at])))))

(deftest claim-tasks-on-a-missing-queue-is-an-error
  (setup!)
  (is (= "42P01" (sqlstate #(tx (fn [c] (engine/claim-tasks c t0 "missing" "w" 60 1)))))))

(deftest claim-tasks-fails-an-expired-lease-with-claim-timeout-and-retries
  (setup!)
  (let [spawned (spawn! t0 {"max_attempts" 3})
        [claim] (claim! t0 1 10)]
    (is (= (:run-id spawned) (:run-id claim)))
    (testing "a lease that has not expired is left alone"
      (is (empty? (claim! (+ t0 9999) 1))))
    (testing "an expired lease fails the run and the retry is claimed in the same call"
      (let [[retry] (claim! (+ t0 10000) 1)
            failed (run-row (:run-id spawned))]
        (is (= (:task-id spawned) (:task-id retry)))
        (is (= 2 (:attempt retry)))
        (is (not= (:run-id spawned) (:run-id retry)))
        (is (= "failed" (:state failed)))
        (is (= {:name "$ClaimTimeout"
                :message "worker did not finish task within claim interval"
                :workerId "w1"
                :claimExpiredAt (str (engine/ms->instant (+ t0 10000)))
                :attempt 1}
               (engine/decode-json (:failure-reason failed))))
        (is (= {:state "running"
                :attempts 2}
               (select-keys (task-row (:task-id spawned)) [:state :attempts])))))))

(deftest claim-tasks-fails-the-task-when-an-expired-lease-was-the-last-attempt
  (setup!)
  (let [spawned (spawn! t0 {"max_attempts" 1})]
    (claim! t0 1 10)
    (is (empty? (claim! (+ t0 10000) 1)))
    (is (= "failed" (:state (task-row (:task-id spawned)))))))

(deftest claim-tasks-bounds-the-expired-lease-sweep-by-qty
  (setup!)
  (let [spawned (mapv #(spawn! (+ t0 %)) (range 3))]
    (claim! (+ t0 5) 3 10)
    (claim! (+ t0 20000) 1 10)
    (is (= 1 (count (filter #(= "failed" (:state (run-row (:run-id %)))) spawned)))
        "one expired lease per requested claim")))

(deftest claim-tasks-applies-max-delay-before-claiming
  (setup!)
  (let [delayed (spawn! t0 {"cancellation" {"max_delay" 60}})]
    (delay-run! (:run-id delayed) (+ t0 1000000))
    (is (empty? (claim! (+ t0 59999))))
    (is (= "pending" (:state (task-row (:task-id delayed)))) "still within max_delay")
    (is (empty? (claim! (+ t0 60000))))
    (is (= {:state "cancelled"
            :cancelled-at (+ t0 60000)}
           (select-keys (task-row (:task-id delayed)) [:state :cancelled-at])))
    (is (= "cancelled" (:state (run-row (:run-id delayed)))))))

(deftest claim-tasks-applies-max-duration-to-a-running-task
  (setup!)
  (let [timed (spawn! t0 {"cancellation" {"max_duration" 10}})]
    (claim! t0 1 600)
    (is (= "running" (:state (run-row (:run-id timed)))))
    (claim! (+ t0 9999))
    (is (= "running" (:state (task-row (:task-id timed)))) "still within max_duration")
    (claim! (+ t0 10000))
    (is (= "cancelled" (:state (task-row (:task-id timed)))))
    (is (= {:state "cancelled"
            :claimed-by nil
            :claim-expires-at nil}
           (select-keys (run-row (:run-id timed)) [:state :claimed-by :claim-expires-at])))))

(deftest complete-run-stores-the-result-and-completes-the-task
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0)]
    (claim! t0)
    (tx #(engine/complete-run % (+ t0 5) "q" run-id {:done true}))
    (is (= {:state "completed"
            :completed-at (+ t0 5)
            :result "{\"done\":true}"}
           (select-keys (run-row run-id) [:state :completed-at :result])))
    (is (= {:state "completed"
            :completed-payload "{\"done\":true}"
            :last-attempt-run run-id}
           (select-keys (task-row task-id) [:state :completed-payload :last-attempt-run])))
    (testing "a nil result is the JSON text null"
      (let [other (spawn! t0)]
        (claim! t0)
        (tx #(engine/complete-run % t0 "q" (:run-id other) nil))
        (is (= "null" (:result (run-row (:run-id other)))))))))

(deftest complete-run-rejects-runs-that-are-not-running
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0 {"max_attempts" 1})
        complete! #(tx (fn [c] (engine/complete-run c t0 "q" run-id {})))]
    (is (= "Run \"" (subs (message complete!) 0 5)))
    (is (= (str "Run \"" run-id "\" is not currently running in queue \"q\"") (message complete!)))
    (claim! t0)
    (tx #(engine/fail-run % t0 "q" run-id {:name "E"}))
    (is (= "AB002" (sqlstate complete!)))
    (is (= (str "Run \"" run-id "\" has already failed in queue \"q\"") (message complete!)))
    (testing "a cancelled run raises AB001"
      (let [other (spawn! t0)]
        (claim! t0)
        (tx #(engine/cancel-task % t0 "q" (:task-id other)))
        (let [f #(tx (fn [c] (engine/complete-run c t0 "q" (:run-id other) {})))]
          (is (= "AB001" (sqlstate f)))
          (is (= "Task has been cancelled" (message f))))))
    (testing "an unknown run"
      (let [unknown (str (random-uuid))]
        (is (= (str "Run \"" unknown "\" not found in queue \"q\"")
               (message #(tx (fn [c] (engine/complete-run c t0 "q" unknown {}))))))))
    (is (= "failed" (:state (task-row task-id))))))

(deftest fail-run-retries-with-the-strategy-delay-then-fails-the-task
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0 {"max_attempts" 3
                                             "retry_strategy" {"kind" "exponential"
                                                               "base_seconds" 10}})]
    (claim! t0)
    (tx #(engine/fail-run % t0 "q" run-id {:name "E"
                                           :message "first"}))
    (let [[first-run second-run] (runs-of task-id)]
      (is (= {:state "failed"
              :failed-at t0
              :failure-reason "{\"name\":\"E\",\"message\":\"first\"}"}
             (select-keys first-run [:state :failed-at :failure-reason])))
      (is (= {:attempt 2
              :state "sleeping"
              :available-at (+ t0 10000)}
             (select-keys second-run [:attempt :state :available-at])))
      (is (= {:state "sleeping"
              :attempts 2
              :last-attempt-run (:run-id second-run)}
             (select-keys (task-row task-id) [:state :attempts :last-attempt-run])))
      (testing "the second failure doubles the delay"
        (claim! (+ t0 10000))
        (tx #(engine/fail-run % (+ t0 10000) "q" (:run-id second-run) {:name "E"}))
        (is (= (+ t0 10000 20000) (:available-at (nth (runs-of task-id) 2)))))
      (testing "the last attempt fails the task"
        (let [third (:run-id (nth (runs-of task-id) 2))]
          (claim! (+ t0 30000))
          (tx #(engine/fail-run % (+ t0 30000) "q" third {:name "E"}))
          (is (= 3 (count (runs-of task-id))))
          (is (= {:state "failed"
                  :attempts 3
                  :last-attempt-run third}
                 (select-keys (task-row task-id) [:state :attempts :last-attempt-run]))))))))

(deftest fail-run-without-a-strategy-retries-immediately
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0)]
    (claim! t0)
    (tx #(engine/fail-run % t0 "q" run-id {:name "E"}))
    (is (= {:state "pending"
            :available-at t0}
           (select-keys (second (runs-of task-id)) [:state :available-at])))
    (is (= "pending" (:state (task-row task-id))))))

(deftest fail-run-cancels-the-task-when-the-retry-would-exceed-max-duration
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0 {"retry_strategy" {"kind" "fixed"
                                                               "base_seconds" 30}
                                             "cancellation" {"max_duration" 20}})]
    (claim! t0)
    (tx #(engine/fail-run % (+ t0 1000) "q" run-id {:name "E"}))
    (is (= 1 (count (runs-of task-id))))
    (is (= {:state "cancelled"
            :cancelled-at (+ t0 1000)
            :last-attempt-run run-id}
           (select-keys (task-row task-id) [:state :cancelled-at :last-attempt-run])))))

(deftest fail-run-accepts-a-max-duration-beyond-32-bits
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0 {"retry_strategy" {"kind" "fixed"
                                                               "base_seconds" 30}
                                             "cancellation" {"max_duration" 9223372036854775807}})]
    (claim! t0)
    (tx #(engine/fail-run % (+ t0 1000) "q" run-id {:name "E"}))
    (is (= "sleeping" (:state (task-row task-id))) "retried, not cancelled")
    (is (= 2 (count (runs-of task-id))))
    (testing "claiming with such a task does not abort"
      (is (empty? (claim! (+ t0 1000)))))))

(deftest fail-run-with-an-invalid-stored-strategy-fails-the-task-permanently
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0)]
    (jdbc/execute! *ds* ["update absurd_tasks set retry_strategy = '{\"kind\":\"bogus\"}' where task_id = ?" task-id])
    (claim! t0)
    (tx #(engine/fail-run % t0 "q" run-id {:name "E"}))
    (is (= 1 (count (runs-of task-id))))
    (is (= "failed" (:state (task-row task-id))))))

(deftest fail-run-clears-waits-and-rejects-bad-runs
  (setup!)
  (let [{:keys [run-id]} (spawn! t0 {"max_attempts" 1})]
    (jdbc/execute! *ds* ["insert into absurd_waits (queue_name, task_id, run_id, step_name, event_name, created_at) values ('q', 't', ?, 's', 'e', 0)" run-id])
    (claim! t0)
    (tx #(engine/fail-run % t0 "q" run-id {:name "E"}))
    (is (zero? (:n (first (query "select count(*) as n from absurd_waits")))))
    (is (= "AB002" (sqlstate #(tx (fn [c] (engine/fail-run c t0 "q" run-id {}))))))
    (let [unknown (str (random-uuid))]
      (is (= (str "Run \"" unknown "\" cannot be failed in queue \"q\"")
             (message #(tx (fn [c] (engine/fail-run c t0 "q" unknown {})))))))
    (let [other (spawn! t0)]
      (is (= (str "Run \"" (:run-id other) "\" cannot be failed in queue \"q\"")
             (message #(tx (fn [c] (engine/fail-run c t0 "q" (:run-id other) {})))))
          "a pending run cannot be failed"))))

(deftest schedule-run-after-sleeps-the-run-relative-to-the-engine-clock
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0)]
    (claim! t0)
    (tx #(engine/schedule-run-after % (+ t0 5) "q" run-id 90))
    (is (= {:state "sleeping"
            :claimed-by nil
            :claim-expires-at nil
            :available-at (+ t0 5 90000)
            :wake-event nil}
           (select-keys (run-row run-id) [:state :claimed-by :claim-expires-at :available-at :wake-event])))
    (is (= "sleeping" (:state (task-row task-id))))
    (testing "fractional seconds keep millisecond precision"
      (let [other (spawn! t0)]
        (claim! t0)
        (tx #(engine/schedule-run-after % t0 "q" (:run-id other) 0.25))
        (is (= (+ t0 250) (:available-at (run-row (:run-id other)))))))
    (testing "only a running run can sleep"
      (is (= (str "Run \"" run-id "\" is not currently running in queue \"q\"")
             (message #(tx (fn [c] (engine/schedule-run-after c t0 "q" run-id 1)))))))))

(deftest extend-claim-moves-the-lease-and-validates-the-run
  (setup!)
  (let [{:keys [run-id task-id]} (spawn! t0)]
    (claim! t0 1 10)
    (tx #(engine/extend-claim % (+ t0 4000) "q" run-id 30))
    (is (= (+ t0 4000 30000) (:claim-expires-at (run-row run-id))))
    (is (= "extend_by must be > 0" (message #(tx (fn [c] (engine/extend-claim c t0 "q" run-id 0))))))
    (is (= "extend_by must be > 0" (message #(tx (fn [c] (engine/extend-claim c t0 "q" run-id nil))))))
    (let [unknown (str (random-uuid))]
      (is (= (str "Run \"" unknown "\" not found in queue \"q\"")
             (message #(tx (fn [c] (engine/extend-claim c t0 "q" unknown 5)))))))
    (testing "a cancelled task raises AB001"
      (tx #(engine/cancel-task % t0 "q" task-id))
      (is (= "AB001" (sqlstate #(tx (fn [c] (engine/extend-claim c t0 "q" run-id 5)))))))
    (testing "a failed run raises AB002 and a pending run is not running"
      (let [failing (spawn! t0 {"max_attempts" 1})]
        (is (= (str "Run \"" (:run-id failing) "\" is not currently running in queue \"q\"")
               (message #(tx (fn [c] (engine/extend-claim c t0 "q" (:run-id failing) 5))))))
        (claim! t0)
        (tx #(engine/fail-run % t0 "q" (:run-id failing) {}))
        (is (= "AB002" (sqlstate #(tx (fn [c] (engine/extend-claim c t0 "q" (:run-id failing) 5))))))))))

(deftest cancel-task-cancels-runs-and-clears-waits
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0)]
    (claim! t0)
    (jdbc/execute! *ds* ["insert into absurd_waits (queue_name, task_id, run_id, step_name, event_name, created_at) values ('q', ?, ?, 's', 'e', 0)" task-id run-id])
    (tx #(engine/cancel-task % (+ t0 7) "q" task-id))
    (is (= {:state "cancelled"
            :cancelled-at (+ t0 7)}
           (select-keys (task-row task-id) [:state :cancelled-at])))
    (is (= {:state "cancelled"
            :claimed-by nil
            :claim-expires-at nil}
           (select-keys (run-row run-id) [:state :claimed-by :claim-expires-at])))
    (is (zero? (:n (first (query "select count(*) as n from absurd_waits")))))
    (testing "cancelling again changes nothing"
      (tx #(engine/cancel-task % (+ t0 99) "q" task-id))
      (is (= (+ t0 7) (:cancelled-at (task-row task-id)))))))

(deftest cancel-task-is-a-noop-on-terminal-tasks-and-errors-on-unknown-ones
  (setup!)
  (let [done (spawn! t0)
        failed (spawn! t0 {"max_attempts" 1})]
    (claim! t0 2)
    (tx #(engine/complete-run % t0 "q" (:run-id done) {}))
    (tx #(engine/fail-run % t0 "q" (:run-id failed) {}))
    (tx #(engine/cancel-task % t0 "q" (:task-id done)))
    (tx #(engine/cancel-task % t0 "q" (:task-id failed)))
    (is (= "completed" (:state (task-row (:task-id done)))))
    (is (= "failed" (:state (task-row (:task-id failed)))))
    (is (nil? (:cancelled-at (task-row (:task-id done)))))
    (let [unknown (str (random-uuid))]
      (is (= (str "Task \"" unknown "\" not found in queue \"q\"")
             (message #(tx (fn [c] (engine/cancel-task c t0 "q" unknown)))))))))

(deftest retry-task-adds-an-attempt-in-place
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0 {"max_attempts" 1})]
    (claim! t0)
    (tx #(engine/fail-run % t0 "q" run-id {:name "E"}))
    (let [retried (tx #(engine/retry-task % (+ t0 100) "q" task-id {}))]
      (is (= {:task-id task-id
              :attempt 2
              :created false}
             (dissoc retried :run-id)))
      (is (= {:attempt 2
              :state "pending"
              :available-at (+ t0 100)}
             (select-keys (run-row (:run-id retried)) [:attempt :state :available-at])))
      (is (= {:state "pending"
              :attempts 2
              :max-attempts 2
              :last-attempt-run (:run-id retried)
              :cancelled-at nil}
             (select-keys (task-row task-id) [:state :attempts :max-attempts :last-attempt-run :cancelled-at])))
      (testing "the retried run can be claimed again"
        (is (= [(:run-id retried)] (mapv :run-id (claim! (+ t0 100)))))))))

(deftest retry-task-honours-max-attempts-and-validates-the-task
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0 {"max_attempts" 2})
        retry! (fn [options] #(tx (fn [c] (engine/retry-task c t0 "q" task-id options))))]
    (is (= (str "Task \"" task-id "\" is not currently failed in queue \"q\"")
           (message (retry! {}))))
    (claim! t0)
    (tx #(engine/fail-run % t0 "q" run-id {}))
    (claim! t0)
    (tx #(engine/fail-run % t0 "q" (:run-id (second (runs-of task-id))) {}))
    (is (= "failed" (:state (task-row task-id))))
    (is (= "max_attempts must be >= 1" (message (retry! {"max_attempts" 0}))))
    (is (= "max_attempts (2) must be greater than current attempts (2)"
           (message (retry! {"max_attempts" 2}))))
    (is (= 3 (:attempt (tx #(engine/retry-task % t0 "q" task-id {"max_attempts" 5})))))
    (is (= 5 (:max-attempts (task-row task-id))))
    (let [unknown (str (random-uuid))]
      (is (= (str "Task \"" unknown "\" not found in queue \"q\"")
             (message #(tx (fn [c] (engine/retry-task c t0 "q" unknown {})))))))))

(deftest retry-task-with-spawn-new-creates-a-fresh-task-from-the-original-inputs
  (setup!)
  (let [{:keys [task-id run-id]} (spawn! t0 "mailer" {"max_attempts" 1
                                                      "headers" {"trace" "t1"
                                                                 "skip" nil}
                                                      "retry_strategy" {"kind" "fixed"}
                                                      "cancellation" {"max_delay" 600}})]
    (claim! t0)
    (tx #(engine/fail-run % t0 "q" run-id {}))
    (let [respawned (tx #(engine/retry-task % (+ t0 5) "q" task-id {"spawn_new" true
                                                                    "max_attempts" 4}))
          fresh (task-row (:task-id respawned))]
      (is (true? (:created respawned)))
      (is (= 1 (:attempt respawned)))
      (is (not= task-id (:task-id respawned)))
      (is (= {:task-name "mailer"
              :params "{\"n\":1}"
              :headers "{\"trace\":\"t1\"}"
              :retry-strategy "{\"kind\":\"fixed\"}"
              :max-attempts 4
              :cancellation "{\"max_delay\":600}"
              :state "pending"
              :enqueue-at (+ t0 5)}
             (select-keys fresh [:task-name :params :headers :retry-strategy :max-attempts
                                 :cancellation :state :enqueue-at])))
      (is (= "failed" (:state (task-row task-id))) "the original task is untouched"))))

;; Checkpoints and events

(defn- checkpoint!
  ([task-id run-id step value] (checkpoint! t0 task-id run-id step value nil))
  ([now task-id run-id step value extend-by]
   (tx #(engine/set-task-checkpoint-state % now "q" task-id step value run-id extend-by))))

(defn- checkpoint-row
  [task-id step]
  (first (query "select * from absurd_checkpoints where task_id = ? and checkpoint_name = ?" task-id step)))

(defn- await!
  ([now task-id run-id event] (await! now task-id run-id event nil))
  ([now task-id run-id event timeout]
   (tx #(engine/await-event % now "q" task-id run-id (str "$awaitEvent:" event) event timeout))))

(defn- emit!
  [now event payload]
  (tx #(engine/emit-event % now "q" event payload)))

(defn- event-row
  [event]
  (first (query "select * from absurd_events where event_name = ?" event)))

(defn- claimed-task!
  "Spawn and claim a task at `t0`; returns the spawn result."
  ([] (claimed-task! nil))
  ([options]
   (let [spawned (spawn! t0 options)]
     (claim! t0 1 600)
     spawned)))

(deftest set-task-checkpoint-state-writes-and-reads-a-checkpoint
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)]
    (checkpoint! (+ t0 5) task-id run-id "step-1" {:ok true} nil)
    (is (= {:state "{\"ok\":true}"
            :status "committed"
            :owner-run-id run-id
            :updated-at (+ t0 5)}
           (select-keys (checkpoint-row task-id "step-1") [:state :status :owner-run-id :updated-at])))
    (is (= {:checkpoint-name "step-1"
            :state {:ok true}
            :status "committed"
            :owner-run-id run-id
            :updated-at (engine/ms->instant (+ t0 5))}
           (tx #(engine/get-task-checkpoint-state % t0 "q" task-id "step-1"))))
    (is (nil? (tx #(engine/get-task-checkpoint-state % t0 "q" task-id "missing"))))
    (testing "the same run can overwrite its own checkpoint"
      (checkpoint! (+ t0 6) task-id run-id "step-1" {:ok false} nil)
      (is (= {:ok false} (:state (tx #(engine/get-task-checkpoint-state % t0 "q" task-id "step-1"))))))
    (testing "a nil value is the JSON text null, and still counts as a checkpoint"
      (checkpoint! task-id run-id "nothing" nil)
      (is (= "null" (:state (checkpoint-row task-id "nothing"))))
      (is (= {:state nil}
             (select-keys (tx #(engine/get-task-checkpoint-state % t0 "q" task-id "nothing")) [:state]))))))

(deftest set-task-checkpoint-state-validates-the-run-and-task
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task! {"max_attempts" 1})
        write! #(tx (fn [c] (engine/set-task-checkpoint-state c t0 "q" task-id %1 %3 %2 nil)))]
    (is (= "step_name must be provided" (message #(write! "  " run-id 1))))
    (is (= "step_name must be provided" (message #(write! nil run-id 1))))
    (let [unknown (str (random-uuid))]
      (is (= (str "Run \"" unknown "\" not found for checkpoint") (message #(write! "s" unknown 1)))))
    (testing "a failed run raises AB002"
      (tx #(engine/fail-run % t0 "q" run-id {}))
      (is (= "AB002" (sqlstate #(write! "s" run-id 1))))
      (is (= (str "Run \"" run-id "\" has already failed in queue \"q\"") (message #(write! "s" run-id 1)))))
    (testing "a cancelled task raises AB001"
      (let [other (claimed-task!)]
        (tx #(engine/cancel-task % t0 "q" (:task-id other)))
        (let [f #(tx (fn [c] (engine/set-task-checkpoint-state c t0 "q" (:task-id other) "s" 1 (:run-id other) 60)))]
          (is (= "AB001" (sqlstate f)))
          (is (= "Task has been cancelled" (message f))))))
    (is (zero? (:n (first (query "select count(*) as n from absurd_checkpoints")))) "nothing was written")))

(deftest set-task-checkpoint-state-extends-the-claim-when-asked
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)]
    (is (= (+ t0 600000) (:claim-expires-at (run-row run-id))))
    (checkpoint! (+ t0 1000) task-id run-id "a" 1 nil)
    (is (= (+ t0 600000) (:claim-expires-at (run-row run-id))) "no extension requested")
    (checkpoint! (+ t0 1000) task-id run-id "b" 1 0)
    (is (= (+ t0 600000) (:claim-expires-at (run-row run-id))) "zero does not extend")
    (checkpoint! (+ t0 1000) task-id run-id "c" 1 30)
    (is (= (+ t0 1000 30000) (:claim-expires-at (run-row run-id))))))

(deftest a-checkpoint-from-a-later-attempt-is-not-overwritten-by-an-earlier-one
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task! {"max_attempts" 3})]
    (tx #(engine/fail-run % t0 "q" run-id {}))
    (let [[second-claim] (claim! t0 1 600)]
      (is (= 2 (:attempt second-claim)))
      (checkpoint! task-id (:run-id second-claim) "s" "from attempt 2")
      (checkpoint! task-id (:run-id second-claim) "only-later" "kept")
      (testing "a zombie attempt 1 write is ignored"
        (jdbc/execute! *ds* ["update absurd_runs set state = 'running' where run_id = ?" run-id])
        (checkpoint! task-id run-id "s" "from attempt 1")
        (is (= {:state "\"from attempt 2\""
                :owner-run-id (:run-id second-claim)}
               (select-keys (checkpoint-row task-id "s") [:state :owner-run-id]))))
      (testing "an earlier run does not see later checkpoints, a later run sees both"
        (is (empty? (tx #(engine/get-task-checkpoint-states % t0 "q" task-id run-id))))
        (is (= #{"s" "only-later"}
               (set (map :checkpoint-name
                         (tx #(engine/get-task-checkpoint-states % t0 "q" task-id (:run-id second-claim)))))))))))

(deftest get-task-checkpoint-states-orders-by-update-time-and-validates-the-run
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)
        other (spawn! t0)]
    (checkpoint! (+ t0 30) task-id run-id "c" 3 nil)
    (checkpoint! (+ t0 10) task-id run-id "a" 1 nil)
    (checkpoint! (+ t0 20) task-id run-id "b" 2 nil)
    (is (= [{:checkpoint-name "a"
             :state 1
             :status "committed"
             :owner-run-id run-id
             :updated-at (engine/ms->instant (+ t0 10))}
            {:checkpoint-name "b"
             :state 2
             :status "committed"
             :owner-run-id run-id
             :updated-at (engine/ms->instant (+ t0 20))}
            {:checkpoint-name "c"
             :state 3
             :status "committed"
             :owner-run-id run-id
             :updated-at (engine/ms->instant (+ t0 30))}]
           (tx #(engine/get-task-checkpoint-states % t0 "q" task-id run-id))))
    (let [unknown (str (random-uuid))]
      (is (= (str "Run \"" unknown "\" not found in queue \"q\"")
             (message #(tx (fn [c] (engine/get-task-checkpoint-states c t0 "q" task-id unknown)))))))
    (is (= (str "Run \"" (:run-id other) "\" does not belong to task \"" task-id "\" in queue \"q\"")
           (message #(tx (fn [c] (engine/get-task-checkpoint-states c t0 "q" task-id (:run-id other)))))))))

(deftest await-event-suspends-the-run-and-registers-a-wait
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)]
    (is (= {:should-suspend true
            :payload nil}
           (await! (+ t0 10) task-id run-id "ping")))
    (is (= {:state "sleeping"
            :claimed-by nil
            :claim-expires-at nil
            :available-at engine/infinity
            :wake-event "ping"
            :event-payload nil}
           (select-keys (run-row run-id) [:state :claimed-by :claim-expires-at :available-at
                                          :wake-event :event-payload])))
    (is (= "sleeping" (:state (task-row task-id))))
    (is (= [{:task-id task-id
             :run-id run-id
             :step-name "$awaitEvent:ping"
             :event-name "ping"
             :timeout-at nil
             :created-at (+ t0 10)}]
           (map #(dissoc % :queue-name) (query "select * from absurd_waits"))))
    (testing "the not-emitted sentinel is a row with a NULL payload"
      (is (= {:payload nil
              :emitted-at 0}
             (select-keys (event-row "ping") [:payload :emitted-at]))))
    (testing "a sleeping run without a timeout is never claimed"
      (is (empty? (claim! (+ t0 100000000000)))))))

(deftest await-event-with-a-timeout-wakes-at-the-deadline
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)]
    (await! (+ t0 10) task-id run-id "ping" 30)
    (is (= (+ t0 10 30000) (:available-at (run-row run-id))))
    (is (= (+ t0 10 30000) (:timeout-at (first (query "select timeout_at from absurd_waits")))))))

(deftest await-event-validates-its-input
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)]
    (is (= "event_name must be provided"
           (message #(tx (fn [c] (engine/await-event c t0 "q" task-id run-id "s" " " nil))))))
    (is (= "timeout must be non-negative" (message #(await! t0 task-id run-id "ping" -1))))
    (let [unknown (str (random-uuid))]
      (is (= (str "Run \"" unknown "\" not found while awaiting event")
             (message #(await! t0 task-id unknown "ping")))))
    (testing "a run that is not running cannot wait"
      (await! t0 task-id run-id "ping")
      (is (= (str "Run \"" run-id "\" must be running to await events")
             (message #(tx (fn [c] (engine/await-event c t0 "q" task-id run-id "other-step" "pong" nil)))))))
    (testing "a cancelled task raises AB001"
      (let [other (claimed-task!)]
        (tx #(engine/cancel-task % t0 "q" (:task-id other)))
        (is (= "AB001" (sqlstate #(await! t0 (:task-id other) (:run-id other) "ping"))))))))

(deftest emit-event-wakes-the-waiter-writes-the-checkpoint-and-deletes-the-wait
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)]
    (await! (+ t0 10) task-id run-id "ping" 600)
    (emit! (+ t0 20) "ping" {:n 1})
    (is (= {:payload "{\"n\":1}"
            :emitted-at (+ t0 20)}
           (select-keys (event-row "ping") [:payload :emitted-at])))
    (is (= {:state "pending"
            :available-at (+ t0 20)
            :wake-event nil
            :event-payload "{\"n\":1}"
            :claimed-by nil}
           (select-keys (run-row run-id) [:state :available-at :wake-event :event-payload :claimed-by])))
    (is (= "pending" (:state (task-row task-id))))
    (is (= {:state "{\"n\":1}"
            :status "committed"
            :owner-run-id run-id
            :updated-at (+ t0 20)}
           (select-keys (checkpoint-row task-id "$awaitEvent:ping") [:state :status :owner-run-id :updated-at])))
    (is (zero? (:n (first (query "select count(*) as n from absurd_waits")))))
    (testing "the woken run is claimed with the payload and resolves the await"
      (let [[claim] (claim! (+ t0 20) 1 600)]
        (is (= run-id (:run-id claim)))
        (is (nil? (:wake-event claim)))
        (is (= {:n 1} (:event-payload claim))))
      (is (= {:should-suspend false
              :payload {:n 1}}
             (await! (+ t0 30) task-id run-id "ping" 600))))))

(deftest emit-event-wakes-every-waiter-of-the-event-and-only-that-event
  (setup!)
  (let [a (claimed-task!)
        b (do (spawn! t0) (first (claim! t0)))
        c (do (spawn! t0) (first (claim! t0)))]
    (await! t0 (:task-id a) (:run-id a) "ping")
    (await! t0 (:task-id b) (:run-id b) "ping")
    (await! t0 (:task-id c) (:run-id c) "other")
    (emit! (+ t0 5) "ping" "go")
    (is (= ["pending" "pending" "sleeping"]
           (mapv #(:state (run-row (:run-id %))) [a b c])))
    (is (= [{:event-name "other"}] (query "select event_name from absurd_waits")))))

(deftest emit-event-first-write-wins
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)]
    (emit! (+ t0 1) "ping" {:value 1})
    (emit! (+ t0 2) "ping" {:value 2})
    (is (= {:payload "{\"value\":1}"
            :emitted-at (+ t0 1)}
           (select-keys (event-row "ping") [:payload :emitted-at])))
    (testing "a later emit does not wake a run that waits afterwards"
      (is (= {:should-suspend false
              :payload {:value 1}}
             (await! (+ t0 3) task-id run-id "ping")))
      (is (= {:state "{\"value\":1}"}
             (select-keys (checkpoint-row task-id "$awaitEvent:ping") [:state]))))))

(deftest await-event-after-an-emit-resolves-immediately
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)]
    (emit! t0 "early" [1 2])
    (is (= {:should-suspend false
            :payload [1 2]}
           (await! (+ t0 5) task-id run-id "early")))
    (is (= "running" (:state (run-row run-id))))
    (is (zero? (:n (first (query "select count(*) as n from absurd_waits")))))
    (testing "an existing checkpoint short-circuits the wait"
      (is (= {:should-suspend false
              :payload [1 2]}
             (await! (+ t0 6) task-id run-id "early"))))))

(deftest emit-event-ignores-waits-that-already-timed-out
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)]
    (await! t0 task-id run-id "ping" 10)
    (emit! (+ t0 10000) "ping" {:late true})
    (is (= "sleeping" (:state (run-row run-id))) "the run still wakes by its own timeout")
    (is (nil? (checkpoint-row task-id "$awaitEvent:ping")))
    (is (zero? (:n (first (query "select count(*) as n from absurd_waits")))) "the expired wait is removed")))

(deftest a-timed-out-wait-resumes-once-without-registering-again
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)]
    (await! t0 task-id run-id "ping" 10)
    (let [[claim] (claim! (+ t0 10000) 1 600)]
      (is (= run-id (:run-id claim)))
      (is (= "ping" (:wake-event claim)))
      (is (nil? (:event-payload claim))))
    (is (zero? (:n (first (query "select count(*) as n from absurd_waits")))) "claiming removes the timed-out wait")
    (is (= {:should-suspend false
            :payload nil}
           (await! (+ t0 10001) task-id run-id "ping" 10)))
    (is (nil? (:wake-event (run-row run-id))))
    (is (= "running" (:state (run-row run-id))))
    (is (zero? (:n (first (query "select count(*) as n from absurd_waits")))))
    (is (nil? (checkpoint-row task-id "$awaitEvent:ping")) "a timeout leaves no checkpoint")))

(deftest a-nil-payload-is-json-null-and-distinct-from-the-not-emitted-sentinel
  (setup!)
  (let [{:keys [task-id run-id]} (claimed-task!)]
    (await! t0 task-id run-id "ping")
    (is (nil? (:payload (event-row "ping"))) "the sentinel is SQL NULL")
    (emit! (+ t0 5) "ping" nil)
    (is (= "null" (:payload (event-row "ping"))) "an emitted nil is the JSON text null")
    (is (= (+ t0 5) (:emitted-at (event-row "ping"))))
    (is (= "pending" (:state (run-row run-id))) "the sentinel does not block the first emit")
    (is (= "null" (:event-payload (run-row run-id))))
    (is (= "null" (:state (checkpoint-row task-id "$awaitEvent:ping"))))
    (testing "the woken run claims a nil payload and resolves the await"
      (let [[claim] (claim! (+ t0 5) 1 600)]
        (is (nil? (:wake-event claim)))
        (is (nil? (:event-payload claim))))
      (is (= {:should-suspend false
              :payload nil}
             (await! (+ t0 6) task-id run-id "ping"))))
    (testing "a second emit is ignored"
      (emit! (+ t0 7) "ping" {:other true})
      (is (= "null" (:payload (event-row "ping")))))))

(deftest emit-event-validates-the-name-and-the-queue
  (setup!)
  (is (= "event_name must be provided" (message #(emit! t0 " " 1))))
  (is (= "event_name must be provided" (message #(emit! t0 nil 1))))
  (is (= "42P01" (sqlstate #(tx (fn [c] (engine/emit-event c t0 "missing" "e" 1)))))))
