(ns absurd.sqlite.schema
  "SQLite DDL for the Absurd tables.

  The tables are static and shared by every queue, keyed by `queue_name`, and
  carry an `absurd_` prefix so they can live in an application's own database
  file. Column names mirror the Postgres per-queue tables. Timestamps are
  `integer` epoch milliseconds, JSON is `text` and UUIDs are `text`."
  (:require [absurd.sqlite.engine :as engine]
            [next.jdbc :as jdbc])
  (:import [java.sql Connection]))

(def schema-version
  "The version of this SQLite schema, stored in `absurd_meta`."
  "1")

(def ^:private run-states
  "check (state in ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled'))")

(def ^:private statements
  ["create table if not exists absurd_meta (
      key text primary key,
      value text not null
    )"

   "create table if not exists absurd_queues (
      queue_name text primary key,
      created_at integer not null,
      storage_mode text not null default 'unpartitioned'
        check (storage_mode in ('unpartitioned', 'partitioned')),
      default_partition text not null default 'enabled'
        check (default_partition in ('enabled', 'disabled')),
      partition_lookahead text not null default '28 days',
      partition_lookback text not null default '1 day',
      cleanup_ttl text not null default '30 days',
      cleanup_limit integer not null default 1000
        check (cleanup_limit >= 1),
      detach_mode text not null default 'none'
        check (detach_mode in ('none', 'empty')),
      detach_min_age text not null default '30 days'
    )"

   (str "create table if not exists absurd_tasks (
      queue_name text not null,
      task_id text not null,
      task_name text not null,
      params text not null,
      headers text,
      retry_strategy text,
      max_attempts integer,
      cancellation text,
      enqueue_at integer not null,
      first_started_at integer,
      state text not null " run-states ",
      attempts integer not null default 0,
      last_attempt_run text,
      completed_payload text,
      cancelled_at integer,
      idempotency_key text,
      primary key (queue_name, task_id),
      unique (queue_name, idempotency_key)
    )")

   (str "create table if not exists absurd_runs (
      queue_name text not null,
      run_id text not null,
      task_id text not null,
      attempt integer not null,
      state text not null " run-states ",
      claimed_by text,
      claim_expires_at integer,
      available_at integer not null,
      wake_event text,
      event_payload text,
      started_at integer,
      completed_at integer,
      failed_at integer,
      result text,
      failure_reason text,
      created_at integer not null,
      primary key (queue_name, run_id)
    )")

   "create table if not exists absurd_checkpoints (
      queue_name text not null,
      task_id text not null,
      checkpoint_name text not null,
      state text,
      status text not null default 'committed',
      owner_run_id text,
      updated_at integer not null,
      primary key (queue_name, task_id, checkpoint_name)
    )"

   "create table if not exists absurd_events (
      queue_name text not null,
      event_name text not null,
      payload text,
      emitted_at integer not null,
      primary key (queue_name, event_name)
    )"

   "create table if not exists absurd_waits (
      queue_name text not null,
      task_id text not null,
      run_id text not null,
      step_name text not null,
      event_name text not null,
      timeout_at integer,
      created_at integer not null,
      primary key (queue_name, run_id, step_name)
    )"

   "create index if not exists absurd_runs_sai on absurd_runs (queue_name, state, available_at)"
   "create index if not exists absurd_runs_ti on absurd_runs (queue_name, task_id)"
   "create index if not exists absurd_runs_cei on absurd_runs (queue_name, claim_expires_at)
      where state = 'running' and claim_expires_at is not null"
   "create index if not exists absurd_waits_eni on absurd_waits (queue_name, event_name)"
   "create index if not exists absurd_waits_ti on absurd_waits (queue_name, task_id)"
   "create index if not exists absurd_events_eai on absurd_events (queue_name, emitted_at)"])

(defn- create-tables!
  [conn]
  (doseq [sql statements]
    (jdbc/execute! conn [sql]))
  (jdbc/execute! conn ["insert or ignore into absurd_meta (key, value) values ('schema_version', ?)"
                       schema-version])
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
