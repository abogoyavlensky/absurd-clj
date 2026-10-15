# Changelog

All notable changes to this project will be documented in this file.

*The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)*

## Unreleased

Targets Absurd schema version 0.5.0.

### Added

- `absurd.core` client: `client`, `close!`, `register-task!`, `spawn!`,
  `fetch-task-result`, `await-task-result`, `emit-event!`, `cancel-task!`,
  `retry-task!`, `create-queue!`, `drop-queue!`, `list-queues`,
  `get-queue-policy`, `set-queue-policy!`, `claim-tasks!`, `work-batch!`,
  `start-worker!`, `stop-worker!`.
- Task context operations: `step`, `begin-step`, `complete-step`,
  `sleep-for`, `sleep-until`, `await-event`, `await-task-result`,
  `heartbeat!`, `emit-event!`, `current-context`.
- Hooks: `:before-spawn`, `:wrap-task-execution`, `:on-lease-timeout`.
- `absurd.sql`: one function per Absurd stored procedure, with
  `AB001`/`AB002` mapped to `:absurd/cancelled` and `:absurd/failed-run`.
- `absurd.store`: a `Store` protocol with one method per stored procedure,
  and `PostgresStore`, which delegates to `absurd.sql`. `absurd.core` reaches
  the database only through it.
- SQLite backend (`absurd.sqlite`, `absurd.sqlite.engine`,
  `absurd.sqlite.schema`): the Absurd procedures ported to Clojure behind the
  `Store` protocol, for single-process apps that want durable execution with one
  database file. Select it with `:db "jdbc:sqlite:..."` or `:backend :sqlite`;
  the schema is created explicitly with `ensure-schema!` or by applying the
  shipped migration `absurd/sqlite/migrations/001-absurd-init.up.sql` with your
  own migration tool; operations without it throw `:absurd/schema-missing`.
  Needs `org.xerial/sqlite-jdbc` on the classpath. No partitioned queues,
  queue policy intervals are stored verbatim, and finished tasks are not
  cleaned up yet.
- `absurd.core/client` options `:backend` and `:db-now-fn`, and the client map
  keys `:store` and `:backend`. New error types `:absurd/invalid-backend`,
  `:absurd/schema-missing` and `:absurd/schema-version-mismatch`.
- `absurd.json`: jsonista codec with keyword keys and jsonb helpers.
- Test suite ported from the Python SDK (see `docs/parity.md`), a replay
  determinism property test, and a cross-SDK check against the Python SDK
  (`bb cross-sdk-test`).
