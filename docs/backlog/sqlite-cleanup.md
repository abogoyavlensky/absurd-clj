# SQLite backend never deletes finished tasks, so the database file only grows

**Status: open**

## Problem

The SQLite backend stores the queue policy fields `cleanup_ttl` and
`cleanup_limit` (`absurd_queues`, `src/absurd/sqlite/schema.clj:34-36`; written
by `set-queue-policy` in `src/absurd/sqlite/engine.clj:378`) but nothing reads
them. Completed, failed and cancelled tasks, their runs, checkpoints and waits,
and emitted events stay in the `absurd_*` tables forever. An app that spawns
tasks continuously grows its database file without bound.

The SDK has no cleanup entry point on either backend: `absurd.core` wraps no
cleanup procedure, and `absurd.store/Store` has no cleanup method. On Postgres
the stored procedures exist (`absurd.cleanup_all_queues`, `absurd.cleanup_tasks`
and `absurd.cleanup_events`, `test/resources/absurd.sql:2044-2284`) and
operators run them from `pg_cron` or `absurdctl`. SQLite has no such
operator, so a single-process app that uses this library as its only
orchestrator has nothing that ever runs a cleanup.

It matters for long-running, high-volume use. A short-lived process, a test
database, or a low-volume app (a few thousand tasks a day) is unaffected for
a long time: rows are small and WAL mode keeps writes cheap.

## Fix

Port `cleanup_tasks` and `cleanup_events` to `absurd.sqlite.engine` and expose
them through the `Store` protocol, so the same API can serve both backends:

- `cleanup-tasks [conn now queue ttl-seconds limit]`: delete tasks whose
  terminal timestamp (`completed_at` / `failed_at` of the last attempt run, or
  `cancelled_at`) is older than the TTL, oldest first, at most `limit`, with
  their waits, checkpoints and runs. Return the number of tasks deleted. The
  non-partitioned branch of the PL/pgSQL is the model.
- `cleanup-events [conn now queue ttl-seconds limit]`: delete emitted events
  older than the TTL, skipping the not-emitted sentinel rows (`payload is
  null`) that `await-event` creates.
- A thin `cleanup-queue` that reads the queue's `cleanup_ttl` and `cleanup_limit`
  and runs both, so a caller (or an optional background timer on the client)
  only needs the queue name.

The policy grammar was chosen with this in mind: `set-queue-policy` accepts only
`<n> second|minute|hour|day|week[s]` and stores the text as given
(`interval-pattern`, `src/absurd/sqlite/engine.clj:362`), so cleanup can parse
`cleanup_ttl` with the same regex and no interval library.

Notes for whoever picks this up:

- Decide the public API first. The SDK exposes no cleanup on Postgres either,
  so adding `absurd.core/cleanup!` is a feature for both backends, not a SQLite
  patch. Check upstream Absurd's SDKs for a name before inventing one.
- Run cleanup in batches inside separate short transactions. One `BEGIN
  IMMEDIATE` holds SQLite's single write lock, so a big delete would stall every
  worker (`busy_timeout` is 3 s). `cleanup_limit` already exists for this.
- An idempotency key lives on the task row, so cleaning a task frees its key,
  as on Postgres. Say so in the docs.
- Cover it in `test/absurd/sqlite/engine_test.clj` with the pinned engine clock
  (`now` is a parameter), and add a parity test in the shared suite if the
  public API lands for Postgres too.
- Expect roughly 80 lines of engine code plus tests.

## Origin

Surfaced while writing the SQLite backend design,
`docs/plans/2026-10-14-1832-sqlite-backend.md` ("Non-goals"): cleanup was left
out because the SDK has no cleanup API on Postgres either, and the backend's job
was parity with the existing API. The README's SQLite section lists it as a
known divergence. Recorded 2026-10-14.
