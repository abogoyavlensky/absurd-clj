(ns absurd.sqlite.schema
  "SQLite DDL for the Absurd tables.

  The tables are static and shared by every queue, keyed by `queue_name`, and
  carry an `absurd_` prefix so they can live in an application's own database
  file. Column names mirror the Postgres per-queue tables. Timestamps are
  `integer` epoch milliseconds, JSON is `text` and UUIDs are `text`.

  The DDL lives in the classpath resource `migration-resource`, the single
  source of truth: `ensure-schema!` runs it, and an application may apply the
  same file with its own migration tool. Future schema versions ship as new
  numbered files (`002-....up.sql`) and never edit 001; `ensure-schema!` will
  then apply the files above the stored version."
  (:require [absurd.sqlite.engine :as engine]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [next.jdbc :as jdbc])
  (:import [java.sql Connection]))

(def schema-version
  "The version of this SQLite schema, stored in `absurd_meta`."
  "1")

(def migration-resource
  "The classpath path of the SQL file that creates schema version 1."
  "absurd/sqlite/migrations/001-absurd-init.up.sql")

;; split like ragtime.sql/read-sql, so both read the file the same way
(def ^:private statements
  (->> (str/split (slurp (io/resource migration-resource)) #"(?m)\n\s*--\s?;;\s*\n")
       (map str/trim)
       (remove str/blank?)
       vec))

(defn- create-tables!
  [conn]
  (doseq [sql statements]
    (jdbc/execute! conn [sql]))
  (let [found (:value (engine/query-one conn ["select value from absurd_meta where key = 'schema_version'"]))]
    (when-not (= schema-version found)
      (throw (ex-info (str "Unsupported absurd SQLite schema version \"" found
                           "\"; this library supports \"" schema-version "\"")
                      {:type :absurd/schema-version-mismatch
                       :expected schema-version
                       :found found})))))

(defn ensure-schema!
  "Create the Absurd tables and indexes when missing, switch the database to
  WAL journaling (the mode persists in the file) and check the stored schema
  version. Safe to call repeatedly. Throws `:absurd/schema-version-mismatch`
  when the file was written by another schema version. `env` is the engine
  environment `{:connectable c :lock l}`."
  [env]
  (engine/call-with-connection
    env
    (fn [^Connection conn]
      (engine/query conn ["pragma journal_mode=wal"])
      (engine/call-in-tx conn :write create-tables!)))
  nil)
