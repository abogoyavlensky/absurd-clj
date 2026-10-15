(ns absurd.client-test
  (:require [absurd.core :as absurd]
            [absurd.test-helpers :as h]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(use-fixtures :each h/with-conn)

(deftest resolve-database-url-follows-upstream-order
  (h/only :postgres
          (is (= "postgresql://localhost/absurd"
                 (absurd/resolve-database-url {"ABSURD_DATABASE_URL" "postgresql://localhost/absurd"
                                               "PGDATABASE" "postgresql://localhost/other"}
                                              nil)))
          (is (= "postgresql://localhost/from-pgdatabase"
                 (absurd/resolve-database-url {"PGDATABASE" "postgresql://localhost/from-pgdatabase"} nil)))
          (is (= "postgresql://localhost/absurd" (absurd/resolve-database-url {} nil)))
          (is (= "postgresql://localhost/absurd" (absurd/resolve-database-url {"ABSURD_DATABASE_URL" ""} nil)))
          (is (= "postgresql://x/y" (absurd/resolve-database-url {"ABSURD_DATABASE_URL" "postgresql://a/b"}
                                                                 "postgresql://x/y")))))

(deftest jdbc-url-conversion
  (h/only :postgres
          (is (= "jdbc:postgresql://localhost/absurd" (absurd/->jdbc-url "postgresql://localhost/absurd")))
          (is (= "jdbc:postgresql://host:5433/db?user=user&password=pw&sslmode=require"
                 (absurd/->jdbc-url "postgresql://user:pw@host:5433/db?sslmode=require")))
          (is (= "jdbc:postgresql://localhost/db" (absurd/->jdbc-url "postgres:///db")))
          (testing "reserved characters in credentials survive the conversion"
            (is (= "jdbc:postgresql://host/db?user=user&password=p%2Bss"
                   (absurd/->jdbc-url "postgresql://user:p+ss@host/db")))
            (is (= "jdbc:postgresql://host/db?user=user&password=p%26ss"
                   (absurd/->jdbc-url "postgresql://user:p&ss@host/db")))
            (is (= "jdbc:postgresql://host/db?user=u%40x&password=p%40ss"
                   (absurd/->jdbc-url "postgresql://u%40x:p%40ss@host/db"))))
          (is (= "jdbc:postgresql://x/y" (absurd/->jdbc-url "jdbc:postgresql://x/y")))
          (is (= "jdbc:postgresql://localhost/mydb" (absurd/->jdbc-url "mydb")))))

(deftest constructor-fails-before-connect-on-empty-queue
  (h/only :postgres
          (let [e (try (absurd/client {:db "postgresql://nowhere.invalid:1/x"
                                       :queue ""})
                       nil
                       (catch clojure.lang.ExceptionInfo e e))]
            (is (= :absurd/invalid-queue-name (:type (ex-data e))))
            (is (str/includes? (ex-message e) "Queue name must be provided")))))

(deftest client-from-url-opens-its-own-datasource
  (h/only :postgres
          (let [app (absurd/client {:db h/jdbc-url
                                    :queue "default"})]
            (is (true? (:owned-db? app)))
            (is (vector? (absurd/list-queues app)))
            (absurd/close! app))))

(deftest client-is-an-open-map
  (let [app (absurd/client (merge (h/client-opts) {:queue "default"
                                                   :max-attempts 7}))]
    (is (= "default" (:queue app)))
    (is (= 7 (:max-attempts app)))
    (is (false? (:owned-db? app)))
    (is (= {} @(:registry app)))
    (is (= (assoc app :extra 1) (merge app {:extra 1})))))

(deftest queue-validation-is-permissive-but-bounded
  (let [app (absurd/client (merge (h/client-opts) {:queue "default"}))]
    (doseq [queue ["Queue Name-1" "   "]]
      (absurd/create-queue! app queue)
      (absurd/emit-event! app "event" {:value 1} {:queue queue})
      (absurd/drop-queue! app queue))
    (let [e (try (absurd/drop-queue! app (apply str (repeat 58 "q")))
                 nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :absurd/invalid-queue-name (:type (ex-data e))))
      (is (str/includes? (ex-message e) "too long")))
    (testing "57 bytes is accepted, multibyte characters count as bytes"
      (is (nil? (h/ex-type #(absurd/validate-queue-name (apply str (repeat 57 "q"))))))
      (is (= :absurd/invalid-queue-name
             (h/ex-type #(absurd/validate-queue-name (apply str (repeat 29 "é")))))))))

(deftest queue-policy-methods
  (let [app (absurd/client (merge (h/client-opts) {:queue "default"}))
        queue (h/random-queue "policy")]
    (absurd/create-queue! app queue {:partition-lookahead "35 days"
                                     :partition-lookback "2 days"
                                     :cleanup-ttl "3 days"
                                     :cleanup-limit 77
                                     :detach-mode :empty
                                     :detach-min-age "45 days"})
    (let [policy (absurd/get-queue-policy app queue)]
      (is (= queue (:queue-name policy)))
      (is (= :unpartitioned (:storage-mode policy)))
      (is (= "35 days" (:partition-lookahead policy)))
      (is (= "2 days" (:partition-lookback policy)))
      (is (= "3 days" (:cleanup-ttl policy)))
      (is (= 77 (:cleanup-limit policy)))
      (is (= :empty (:detach-mode policy)))
      (is (= "45 days" (:detach-min-age policy))))
    (absurd/set-queue-policy! app queue {:cleanup-ttl "4 days"
                                         :cleanup-limit 12})
    (let [updated (absurd/get-queue-policy app queue)]
      (is (= "4 days" (:cleanup-ttl updated)))
      (is (= 12 (:cleanup-limit updated))))
    (testing "string enums and unknown storage modes"
      (absurd/create-queue! app (h/random-queue "policy") {:storage-mode "unpartitioned"})
      (is (= :absurd/invalid-storage-mode
             (h/ex-type #(absurd/create-queue! app (h/random-queue "policy") {:storage-mode :weird})))))
    (is (nil? (absurd/get-queue-policy app (h/random-queue "missing"))))))

(deftest partitioned-queue-policy-normalizes-intervals
  (h/only :postgres
          (let [app (absurd/client (merge (h/client-opts) {:queue "default"}))
                queue (h/random-queue "policy")]
            (absurd/create-queue! app queue {:storage-mode :partitioned
                                             :partition-lookahead "35 days"
                                             :partition-lookback "2 days"
                                             :cleanup-ttl "12345 seconds"
                                             :cleanup-limit 77
                                             :detach-mode :empty
                                             :detach-min-age "45 days"})
            (let [policy (absurd/get-queue-policy app queue)]
              (is (= queue (:queue-name policy)))
              (is (= :partitioned (:storage-mode policy)))
              (is (= "35 days" (:partition-lookahead policy)))
              (is (= "2 days" (:partition-lookback policy)))
              (is (= "03:25:45" (:cleanup-ttl policy)))
              (is (= 77 (:cleanup-limit policy)))
              (is (= :empty (:detach-mode policy)))
              (is (= "45 days" (:detach-min-age policy))))
            (absurd/set-queue-policy! app queue {:cleanup-ttl "4321 seconds"
                                                 :cleanup-limit 12})
            (let [updated (absurd/get-queue-policy app queue)]
              (is (= "01:12:01" (:cleanup-ttl updated)))
              (is (= 12 (:cleanup-limit updated)))))))

(deftest sqlite-queue-policy-stores-intervals-verbatim
  (h/only :sqlite
          (let [app (absurd/client (merge (h/client-opts) {:queue "default"}))
                queue (h/random-queue "policy")]
            (absurd/create-queue! app queue {:partition-lookahead "35 days"
                                             :partition-lookback "2 days"
                                             :cleanup-ttl "12345 seconds"
                                             :cleanup-limit 77
                                             :detach-mode :empty
                                             :detach-min-age "45 days"})
            (let [policy (absurd/get-queue-policy app queue)]
              (is (= :unpartitioned (:storage-mode policy)))
              (is (= "12345 seconds" (:cleanup-ttl policy)) "Postgres would normalize this to 03:25:45")
              (is (= "35 days" (:partition-lookahead policy)))
              (is (= 77 (:cleanup-limit policy))))
            (absurd/set-queue-policy! app queue {:cleanup-ttl "4321 seconds"})
            (is (= "4321 seconds" (:cleanup-ttl (absurd/get-queue-policy app queue))))
            (testing "intervals outside the supported grammar are rejected"
              (is (thrown-with-msg? java.sql.SQLException #"Invalid interval value \"3:25:45\" for cleanup_ttl"
                                    (absurd/set-queue-policy! app queue {:cleanup-ttl "3:25:45"})))
              (is (thrown-with-msg? java.sql.SQLException #"Invalid interval value \"-1 days\" for cleanup_ttl"
                                    (absurd/set-queue-policy! app queue {:cleanup-ttl "-1 days"})))))))

(deftest partitioned-queues-are-rejected-on-sqlite
  (h/only :sqlite
          (let [app (absurd/client (merge (h/client-opts) {:queue "default"}))
                queue (h/random-queue "partitioned")]
            (is (thrown-with-msg? java.sql.SQLException #"Partitioned queues are not supported on SQLite"
                                  (absurd/create-queue! app queue {:storage-mode :partitioned})))
            (is (nil? (absurd/get-queue-policy app queue)) "nothing was created"))))

(deftest sqlite-client-keeps-the-database-it-was-given
  (h/only :sqlite
          (let [app (absurd/client (merge (h/client-opts) {:queue "default"}))]
            (is (= :sqlite (:backend app)))
            (is (identical? h/*conn* (:db app)))
            (is (false? (:owned-db? app))))))
