-- absurd-clj SQLite schema, version 1. Set journal_mode=WAL on the connection
-- (jdbc:sqlite:app.db?journal_mode=WAL): a migration runs in a transaction and cannot switch it.
create table if not exists absurd_meta (
  key text primary key,
  value text not null
);
--;;
create table if not exists absurd_queues (
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
);
--;;
create table if not exists absurd_tasks (
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
  state text not null check (state in ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
  attempts integer not null default 0,
  last_attempt_run text,
  completed_payload text,
  cancelled_at integer,
  idempotency_key text,
  primary key (queue_name, task_id),
  unique (queue_name, idempotency_key)
);
--;;
create table if not exists absurd_runs (
  queue_name text not null,
  run_id text not null,
  task_id text not null,
  attempt integer not null,
  state text not null check (state in ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
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
);
--;;
create table if not exists absurd_checkpoints (
  queue_name text not null,
  task_id text not null,
  checkpoint_name text not null,
  state text,
  status text not null default 'committed',
  owner_run_id text,
  updated_at integer not null,
  primary key (queue_name, task_id, checkpoint_name)
);
--;;
create table if not exists absurd_events (
  queue_name text not null,
  event_name text not null,
  payload text,
  emitted_at integer not null,
  primary key (queue_name, event_name)
);
--;;
create table if not exists absurd_waits (
  queue_name text not null,
  task_id text not null,
  run_id text not null,
  step_name text not null,
  event_name text not null,
  timeout_at integer,
  created_at integer not null,
  primary key (queue_name, run_id, step_name)
);
--;;
create index if not exists absurd_runs_sai on absurd_runs (queue_name, state, available_at);
--;;
create index if not exists absurd_runs_ti on absurd_runs (queue_name, task_id);
--;;
create index if not exists absurd_runs_cei on absurd_runs (queue_name, claim_expires_at)
  where state = 'running' and claim_expires_at is not null;
--;;
create index if not exists absurd_waits_eni on absurd_waits (queue_name, event_name);
--;;
create index if not exists absurd_waits_ti on absurd_waits (queue_name, task_id);
--;;
create index if not exists absurd_events_eai on absurd_events (queue_name, emitted_at);
--;;
insert or ignore into absurd_meta (key, value) values ('schema_version', '1');
