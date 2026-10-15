# SQLite Backend Implementation Plan

> **Status: completed** (2026-10-14). See the completion summary at the end.

> **For agentic workers:** Use executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let absurd-clj run on SQLite as well as Postgres, behind the same public API, so a single-process app can use durable execution with one database file and no database server.

**Tech Stack:** Clojure 1.12, next.jdbc, org.xerial/sqlite-jdbc (user-provided, test alias only), jsonista, clojure.test + eftest, Testcontainers Postgres (existing).

---

## Design

### Why

absurd-clj is a thin layer over Absurd's PL/pgSQL stored procedures (`test/resources/absurd.sql`, schema 0.5.0). The first consumer we have in mind, Carpet (a self-hosted test coverage service), has to ship as one container with one data volume. Bundling a Postgres server into that container works, but it is heavy and makes upgrades awkward. SQLite has no stored procedures, so the engine logic has to move into Clojure for that backend.

Postgres stays the reference backend. The SQLite backend is a port of the same procedures, with the same semantics, checked by running the **existing parity test suite against both backends**.

### The seam

`absurd.core` already reaches the database through exactly one namespace, `absurd.sql`, which has one function per stored procedure (19 of them plus `current-time`). This plan adds a protocol at that boundary:

| Namespace | Responsibility |
|-----------|----------------|
| `absurd.store` (new) | `Store` protocol with one method per `absurd.sql` function, using the same argument order and the same return shapes but taking a store instead of a connectable. Also defines `PostgresStore`, a record over a connectable that delegates every method to `absurd.sql`. |
| `absurd.sql` (unchanged) | Postgres stored procedure wrappers. Stays public and stable. |
| `absurd.sqlite.schema` (new) | SQLite DDL and `ensure-schema!`. |
| `absurd.sqlite.engine` (new) | The ported procedures as Clojure functions over a transaction-scoped connection, plus the transaction macro, time and UUIDv7 helpers. |
| `absurd.sqlite` (new, public) | `SqliteStore` record implementing `Store`, `store` constructor, and `ensure-schema!` re-export. |
| `absurd.core` | Calls `absurd.store` functions on `(:store app)` instead of `absurd.sql` on `(:db app)`. |

Protocol method names and arities mirror `absurd.sql` one to one:

```clojure
(defprotocol Store
  (create-queue! [store queue storage-mode])
  (drop-queue! [store queue])
  (list-queues [store])
  (get-queue-policy [store queue])
  (set-queue-policy! [store queue policy])
  (spawn-task! [store queue task-name params options])
  (retry-task! [store queue task-id options])
  (cancel-task! [store queue task-id])
  (get-task-result [store queue task-id])
  (claim-tasks! [store queue worker-id claim-timeout qty])
  (complete-run! [store queue run-id result])
  (fail-run! [store queue run-id failure])
  (schedule-run-after! [store queue run-id seconds])
  (extend-claim! [store queue run-id seconds])
  (set-task-checkpoint-state! [store queue task-id checkpoint-name value run-id extend-by])
  (get-task-checkpoint-state [store queue task-id checkpoint-name])
  (get-task-checkpoint-states [store queue task-id run-id])
  (await-event [store queue task-id run-id checkpoint-name event-name timeout])
  (emit-event! [store queue event-name payload])
  (current-time [store]))
```

Return shapes are exactly those of `absurd.sql` today. UUIDs are strings, JSON columns are decoded, `:updated-at` is an `Instant`, and policy intervals are strings. Both backends raise errors the same way. AB001 and AB002 become `:absurd/cancelled` and `:absurd/failed-run` ex-info signals, and every other engine error is a `java.sql.SQLException` whose message mirrors the PL/pgSQL `raise exception` text. The SQLite engine throws `(SQLException. msg "AB001")`, `"AB002"`, `"AB003"` (invalid retry strategy) or `"P0001"` (generic raise), and `SqliteStore` wraps each call in the existing `absurd.sql/with-task-state-errors`. Callers therefore catch the same exception types on both backends. Existing tests that assert `SQLException` plus a message substring (`cancel-non-existent-task-errors`, `heartbeat-zero-is-rejected-by-sql`) keep passing unchanged.

### Client

`absurd.core/client` gains two options:

| Key | Default | Meaning |
|-----|---------|---------|
| `:backend` | inferred | `:postgres` or `:sqlite`. A `:db` string starting with `jdbc:sqlite:` infers `:sqlite`. Everything else infers `:postgres`. A DataSource or Connection for SQLite needs `:backend :sqlite`, because the client never connects in its constructor. |
| `:db-now-fn` | `Instant/now` | SQLite only. The engine clock, which plays the role of Postgres `absurd.current_time()`. Tests pin it, and production leaves it alone. |

The client map gains `:store` and keeps `:db` and every existing key. Each `(:db app)` call site in `absurd.core` switches to `(:store app)`. `:backend :sqlite` with a nil `:db` throws `:absurd/invalid-db` (there is no SQLite equivalent of the `ABSURD_DATABASE_URL`/`PGDATABASE` fallback). An unknown `:backend` value throws `:absurd/invalid-backend`.

The client still never connects in its constructor. The SQLite store creates its schema lazily on the first operation (see below).

### SQLite schema

The tables are static and shared, keyed by `queue_name`. They are not per-queue tables like Postgres. Static DDL means no dynamic identifiers, no quoting rules, and a trivial `drop-queue!` (delete the rows). Partitioning is the only reason Postgres uses per-queue tables, and SQLite doesn't need it. Table names carry an `absurd_` prefix so they can live in the application's own database file.

Columns mirror the Postgres column names exactly (plus `queue_name`), so test helpers return the same keys on both backends:

- `absurd_meta (key text primary key, value text)`: holds `schema_version = '1'`.
- `absurd_queues (queue_name pk, created_at, storage_mode, partition_lookahead, partition_lookback, cleanup_ttl, cleanup_limit, detach_mode, detach_min_age)`: interval fields are text with Postgres defaults `'28 days'`, `'1 day'`, `'30 days'`, `1000`, `'none'`, `'30 days'`.
- `absurd_tasks`: the columns of `t_<queue>`. PK `(queue_name, task_id)`, `unique (queue_name, idempotency_key)`. SQLite allows many NULLs in a unique index, which matches Postgres.
- `absurd_runs`: the columns of `r_<queue>`. PK `(queue_name, run_id)`. Indexes `(queue_name, state, available_at)`, `(queue_name, task_id)`, and a partial index on `(queue_name, claim_expires_at) where state = 'running' and claim_expires_at is not null`.
- `absurd_checkpoints`: PK `(queue_name, task_id, checkpoint_name)`.
- `absurd_events`: PK `(queue_name, event_name)`. Index `(queue_name, emitted_at)`.
- `absurd_waits`: PK `(queue_name, run_id, step_name)`. Indexes `(queue_name, event_name)`, `(queue_name, task_id)`.

Value encoding:

- **Timestamps** are `INTEGER` epoch milliseconds. Postgres `'infinity'` (an await without a timeout) is `Long/MAX_VALUE`, and the event sentinel's `'epoch'` is `0`. The engine converts to `Instant` at its boundary, so stores return the same types.
- **JSON** is `TEXT` written with `absurd.json/write-str` and read with `read-str`. SQL `NULL` and the JSON text `null` stay distinct. This matters for the event sentinel: `payload IS NULL` means "not emitted yet", while `emit-event!` with a nil payload stores the text `null`. The same holds for `params` (`coalesce(p_params, 'null')`).
- **UUIDs** are `TEXT`, generated as UUIDv7 (48-bit unix ms, version and variant bits, random rest) to match `absurd.portable_uuidv7()` and keep `order by run_id` time-ordered.

`ensure-schema!` runs `create table/index if not exists` for everything, sets `pragma journal_mode=WAL` (it persists in the file), inserts `schema_version` if missing, and throws when an existing `schema_version` differs. `SqliteStore` calls it once per store instance, lazily, on the first operation, guarded by a `delay`. It is also public, for apps that want to run it at boot.

### SQLite transactions and concurrency

Every engine operation runs inside `with-tx`:

1. Acquire the store's in-process `ReentrantLock`. This serializes operations from worker threads and request threads that share one store, and it is what makes a single shared `java.sql.Connection` safe.
2. Get a connection: borrow from a DataSource (and close it afterwards), or use the given Connection. Throw when `(.getAutoCommit conn)` is false, because the engine manages its own transaction and can't nest inside a caller's.
3. Run `BEGIN IMMEDIATE` for writes or `BEGIN` (deferred) for read-only operations (`get-task-result`, both `get-task-checkpoint-state*`, `list-queues`, `get-queue-policy`). The body runs, then `COMMIT`, or `ROLLBACK` on any throwable before rethrowing.

`BEGIN IMMEDIATE` takes SQLite's write lock up front, so a transaction never fails later on a read-to-write lock upgrade. Other processes or app connections writing to the same file wait up to sqlite-jdbc's `busy_timeout` (3 s by default). A probe on sqlite-jdbc 3.51.2.0 confirmed that manual `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK` work on an autocommit connection. It also confirmed that `insert ... on conflict do update ... where payload is null` reports 0 changes when the `where` blocks it, which `emit-event!` relies on. No `org.sqlite` classes are imported, so the library has no compile-time dependency on the driver.

`for update skip locked` and the lock-ordering comments in the PL/pgSQL have no SQLite equivalent and need none, because only one write transaction runs at a time.

**Supported deployment:** one process writing to the database file, with any number of threads. Several processes still work but contend on the write lock. That is documented, not tested.

### Porting the procedures

`absurd.sqlite.engine` has one function per procedure, each taking `(conn now queue ...)` where `now` is epoch ms read once from `:db-now-fn` per operation, exactly as each PL/pgSQL function reads `absurd.current_time()` once into `v_now`. The executor ports each function from `test/resources/absurd.sql`, keeping branch order, guards and messages:

| Procedure | Lines in `absurd.sql` | Notes for the port |
|-----------|----------------------|--------------------|
| `retry_delay_seconds` | 56–129 | Pure function. Validation raises AB003. Saturate at the cap instead of overflowing. |
| `create_queue`, `drop_queue`, `list_queues` | 367–462 | `partitioned` throws `SQLException` "Partitioned queues are not supported on SQLite". |
| `get_queue_policy`, `set_queue_policy` | 464–720 | Same key whitelist and validation of `cleanup_limit`, `detach_mode` and `default_partition`. Interval values are validated against a narrow grammar: `^\s*(\d+)\s+(second|minute|hour|day|week)s?\s*$`, case-insensitive. Anything else, including negative values, raises `SQLException` "Invalid interval value \"…\" for <key>". The value is stored as given, with no normalization: Postgres turns `"12345 seconds"` into `"03:25:45"`, SQLite keeps `"12345 seconds"`. This is documented, and the grammar is something a future cleanup feature can parse. |
| `get_task_result` | 722–755 | Left join to the last attempt run. |
| `spawn_task` | 757–906 | Unpartitioned path only. Idempotency uses `insert ... on conflict (queue_name, idempotency_key) do nothing` and checks the changes count. |
| `claim_task` | 908–1071 | Four steps in one transaction: cancellation sweep (call the ported `cancel-task`), expired-lease sweep (limit `qty`, ordered by `claim_expires_at, run_id`, calling the ported `fail-run` with the `$ClaimTimeout` payload, `claimExpiredAt` as an ISO-8601 string, nil keys stripped), candidate select plus updates, timed-out wait cleanup. Return the rows ordered by `available_at, run_id`. |
| `complete_run`, `schedule_run`, `fail_run` | 1073–1333 | `fail_run` takes no `p_retry_at` (the SDK always passes null). |
| `retry_task` | 1335–1458 | `spawn_new` calls the ported `spawn-task` inside the same transaction. |
| `set_task_checkpoint_state`, `extend_claim` | 1460–1608 | Same owner-attempt rule for overwriting checkpoints. |
| `get_task_checkpoint_state(s)` | 1610–1690 | The "states" variant hides checkpoints from later attempts and orders by `updated_at`. |
| `await_event`, `emit_event` | 1692–1974 | Replace Postgres jsonb equality with equality of the decoded Clojure values. Keep the timeout-resume detection (`wake_event = event and event_payload is null`). |
| `cancel_task` | 1976–2042 | |

Out of scope, and absent from the SQLite engine: partitions, `cleanup_*`, cron, and detach jobs. The SDK doesn't wrap them on Postgres either.

### Testing strategy

The parity suite is the acceptance gate. `test/absurd/test_helpers.clj` becomes backend-aware:

- `with-conn` runs each test once per backend in `ABSURD_TEST_BACKENDS` (comma-separated, default `postgres,sqlite`), wrapping each run in `(testing (str "backend " b) ...)`. For Postgres it binds a connection as today. For SQLite it creates a fresh temp database file, binds a DataSource for it, and deletes the file plus its `-wal`/`-shm` siblings afterwards.
- Dynamic vars: `*backend*` (`:postgres` or `:sqlite`), `*conn*` (the connectable: a Connection for Postgres, a DataSource for SQLite, kept under the existing name to avoid churn), and `*db-now*` (an atom holding the pinned `Instant` or nil, used on SQLite).
- `set-fake-now!` sets `absurd.fake_now` on Postgres and resets `*db-now*` on SQLite. `fake-now-fn` follows whichever is active.
- `client-opts` returns `{:db *conn* :backend *backend* :db-now-fn ...}` (and `:now-fn` following the fake clock). `test-client` and every test that calls `absurd/client` directly merge it in.
- `datasource` returns the **active backend's** DataSource: the Testcontainers DataSource on Postgres, and the per-test temp-file DataSource on SQLite. Today it always returns Postgres. `worker_test.clj` (`pooled-client`) and the futures in `task_result_test.clj` call it directly, so without this change SQLite runs would still hit Postgres. Those futures open their own connection and call `sql/claim-tasks!`/`sql/complete-run!` on it. They switch to `store/` calls on `(h/store {:db (h/datasource)})`, a store over the same database that is safe to use from another thread.
- `store` returns `(:store (absurd/client (merge (client-opts) opts)))`, so lower-level tests call `absurd.store` functions instead of `absurd.sql`.
- Row helpers (`get-task`, `get-run`, `get-runs`, `get-checkpoints`, `count-tasks`, `count-checkpoints`) query `absurd.t_<queue>` on Postgres or `absurd_tasks where queue_name = ?` on SQLite. Both decode JSON, return UUIDs as strings, return timestamp columns as `Instant`, and drop `queue_name`.
- New helpers replace the raw SQL now inside tests: `count-waits`, `get-event`, `set-task-headers-json!` (writes raw JSON text for the invalid-headers test), `delete-task!` (replaces `absurd.cleanup_tasks` in the child-cleanup test), and `only` (a macro that runs its body only on the named backend).

Postgres-only tests stay Postgres-only through `only` or a Postgres fixture:
- `schema-test`
- `sql-test`'s Postgres wrapper checks
- the partitioned half of `queue-policy-methods`
- the Postgres URL tests in `client-test`
- the cross-SDK check

SQLite gets its own tests in `test/absurd/sqlite_test.clj`:
- schema idempotence and version mismatch
- WAL enabled
- the partitioned queue rejected
- policies stored verbatim
- coexistence with an app table in the same file
- single-Connection mode
- URL inference
- a concurrency stress test: 4 worker threads, 200 tasks, every task completes exactly once and nothing is double-claimed

`ABSURD_TEST_BACKENDS=sqlite bb test` runs the suite without Docker.

### Non-goals (follow-ups)

- **Cleanup on SQLite** (`cleanup_ttl`/`cleanup_limit` enforcement). Long-running SQLite apps will want it. It's a separate feature because the SDK has no cleanup API on Postgres either. Record it in the backlog.
- **Cross-SDK workers on SQLite.** Python and Go SDKs only speak to the Postgres procedures. Document it.
- **Multi-process write throughput tuning.**
- **Making the Postgres driver optional.**

### Dependencies

`org.xerial/sqlite-jdbc` goes in the `:test` alias only. Users add it themselves, the same way they add a Hikari pool. The README states the requirement. This keeps Postgres users from downloading a ~13 MB native jar.

## File Structure

| File | Change |
|------|--------|
| `src/absurd/store.clj` | Create: `Store` protocol, `PostgresStore`, `postgres-store`. |
| `src/absurd/core.clj` | Modify: `:backend`, `:db-now-fn`, `:store` on the client, and the call sites switch from `sql/` to `store/`. |
| `src/absurd/sqlite.clj` | Create: `SqliteStore`, `store`, `ensure-schema!`. |
| `src/absurd/sqlite/schema.clj` | Create: DDL, schema version, `ensure-schema!`. |
| `src/absurd/sqlite/engine.clj` | Create: `with-tx`, time, JSON, UUIDv7 helpers, and the ported procedures. |
| `deps.edn` | Modify: add `org.xerial/sqlite-jdbc` to `:test`. |
| `test/absurd/test_helpers.clj` | Modify: backend-aware fixture and helpers. |
| `test/absurd/sql_test.clj` → `test/absurd/store_test.clj` | Rename: calls go through `absurd.store` on both backends. `with-task-state-errors` unit checks stay. |
| `test/absurd/{events,execution,heartbeat,retry,sleep,task_result,worker,client,schema}_test.clj` | Modify: raw SQL and `Timestamp` casts go through helpers, `sql/` becomes `store/`, direct `absurd/client` calls merge `client-opts`. |
| `test/absurd/sqlite/engine_test.clj` | Create: focused engine tests per ported group. |
| `test/absurd/sqlite_test.clj` | Create: SQLite-specific behavior and the stress test. |
| `README.md`, `CHANGELOG.md`, `docs/parity.md` | Modify: SQLite section, changelog entry, note that the suite runs on both backends. |
| `docs/backlog/` | Add an entry for SQLite cleanup with the backlog skill (it bootstraps the directory). |

## Tasks

Run tests with `bb test` (both backends, needs Docker) or `ABSURD_TEST_BACKENDS=sqlite bb test`. The second only works from Task 8, when the fixture learns the `sqlite` branch. Before that, the SQLite tests in Tasks 3–7 run one namespace at a time. They set up their own temp-file databases and don't use `with-conn`, so they need neither Docker nor the fixture:

```bash
clojure -M:dev:test -e "(require 'absurd.sqlite.engine-test) (let [r (clojure.test/run-tests 'absurd.sqlite.engine-test)] (System/exit (if (clojure.test/successful? r) 0 1)))"
```

Substitute `absurd.sqlite-test` for Task 7. Use /nrepl-eval for faster iteration when a REPL is running.

### Task 1: Store protocol and Postgres store

**Files:**
- Create: `src/absurd/store.clj`
- Modify: `src/absurd/core.clj`

- [x] **Step 1: Write `absurd.store`**
  The protocol exactly as in the design, plus `(defrecord PostgresStore [db])` delegating each method to the matching `absurd.sql` function with `db` as the connectable, and a `postgres-store` constructor.

- [x] **Step 2: Switch `absurd.core` to the store**
  `client` builds `:store (store/postgres-store connectable)` and keeps `:db`. Replace every `sql/<fn> (:db app)` and `(ctx-db ctx)` call with `store/<fn> (:store app)` (and a `ctx-store` helper). `absurd.core` no longer requires `absurd.sql`.

- [x] **Step 3: Run the suite**
  Run: `bb test`
  Expected: PASS with no test changes. This is a pure refactor.

- [x] **Step 4: Commit**
  `git commit -m "Route database access through a Store protocol"`

### Task 2: Backend-neutral test helpers (Postgres only for now)

**Files:**
- Modify: `test/absurd/test_helpers.clj` and the test files listed in File Structure
- Rename: `test/absurd/sql_test.clj` → `test/absurd/store_test.clj`

- [x] **Step 1: Add the helper surface**
  `*backend*`, `*db-now*`, `client-opts`, `store`, a backend-aware `datasource`, `only`, `count-waits`, `get-event`, `set-task-headers-json!`, `delete-task!`, and timestamp columns returned as `Instant` from the row helpers. `with-conn` reads `ABSURD_TEST_BACKENDS` but only the `postgres` branch exists yet. An unknown backend name throws.

- [x] **Step 2: Port the tests to the helpers**
  - Replace every `(.toInstant ^java.sql.Timestamp ...)` with the `Instant` the helper already returns.
  - Replace every raw `jdbc/execute` against `absurd.*_<queue>` with a helper.
  - Replace every `sql/<fn> h/*conn*` with `store/<fn> (h/store)`, or with `(:store app)` where an app is in scope.
  - Merge `(h/client-opts)` into every direct `absurd/client` call.
  - Wrap the Postgres-only tests listed in the design in `h/only :postgres`.

- [x] **Step 3: Run the suite**
  Run: `bb test`
  Expected: PASS, the same test count as before.

- [x] **Step 4: Commit**
  `git commit -m "Make the test helpers backend-neutral"`

> Deviation: `ABSURD_TEST_BACKENDS` defaults to `postgres` for now, because the `sqlite` branch does not exist until Task 8. Task 8 flips the default to `postgres,sqlite`.
> Deviation: `queue-policy-methods` is split into a backend-neutral test (unpartitioned queue, verbatim intervals) and a Postgres-only `partitioned-queue-policy-normalizes-intervals` with the original assertions. The suite has 115 tests instead of 114.
> Deviation: `delete-task!` deletes the task's rows directly and returns the count, instead of calling `absurd.cleanup_tasks`, so the same helper works on both backends.
> Deviation: `fake-now-fn` takes no argument and follows the active backend; its only caller was `test-client`.

### Task 3: SQLite schema and engine foundation

**Files:**
- Modify: `deps.edn` (add `org.xerial/sqlite-jdbc` to `:test`; use the latest release, checked with `bb outdated-check`)
- Create: `src/absurd/sqlite/schema.clj`, `src/absurd/sqlite/engine.clj`
- Test: `test/absurd/sqlite/engine_test.clj`

- [x] **Step 1: Write tests**
  On a temp-file DataSource:
  - `ensure-schema!` is idempotent and creates all `absurd_*` tables.
  - A different stored `schema_version` throws.
  - `journal_mode` is `wal`.
  - `with-tx` rolls back when the body throws, and throws on a connection with autocommit off.
  - `uuidv7` yields version-7 UUIDs that sort by generation time across milliseconds.
  - `retry-delay-seconds` matches the Postgres function for `nil`, `none`, `fixed` (default 60), `exponential` (default base 30, factor 2, cap) and overflow saturation. Invalid input raises an `SQLException` with SQLState `AB003`.

- [x] **Step 2: Run, expect failure**
  Run: the focused `absurd.sqlite.engine-test` command (see the top of Tasks).
  Expected: FAIL (namespaces missing)

- [x] **Step 3: Implement**
  - Schema per the design.
  - In the engine:
    - `with-tx` (lock, connection, `BEGIN IMMEDIATE`/`BEGIN`, commit/rollback)
    - `raise!` (an `SQLException` with message and SQLState)
    - ms↔`Instant` helpers, with `Long/MAX_VALUE` as infinity
    - JSON column encode/decode that keeps SQL NULL distinct from JSON `null`
    - `uuidv7`
    - `retry-delay-seconds`

- [x] **Step 4: Run, expect pass**
  Run: the focused `absurd.sqlite.engine-test` command (see the top of Tasks).
  Expected: PASS

- [x] **Step 5: Commit**
  `git commit -m "Add SQLite schema and engine foundation"`

> Deviation: sqlite-jdbc is pinned to `3.53.4.0`, the latest release per `bb outdated-check`; the plan's probe ran on 3.51.2.0. The engine tests exercise the same behaviours on the newer driver.
> Deviation: `absurd_queues` also has a `default_partition` column, because `set_queue_policy` validates that key. `ensure-schema!` takes the engine environment `{:connectable c :lock l}` (the public `absurd.sqlite/ensure-schema!` in Task 7 builds one from a bare connectable), because it must hold the store lock and run the WAL pragma outside the transaction.
> Deviation: besides `with-tx`, the engine has `with-read-tx` for the deferred `BEGIN` (a single macro with an optional mode cannot be linted as `let`; `.clj-kondo/config.edn` gains `:lint-as` entries for both). Schema version mismatches throw ex-info `:absurd/schema-version-mismatch`.
> Deviation: `uuidv7` takes the engine clock (`now` ms, like the PL/pgSQL one) and keeps ids strictly increasing within the process, bumping the embedded millisecond when the clock does not advance. This keeps `order by run_id` in creation order even for tasks spawned within one millisecond or under a pinned test clock.

### Task 4: Queues, spawn and task results

**Files:**
- Modify: `src/absurd/sqlite/engine.clj`
- Test: `test/absurd/sqlite/engine_test.clj`

- [x] **Step 1: Write tests**
  - `create-queue` is idempotent, and `partitioned` is rejected.
  - `drop-queue` removes the queue's rows from every table and leaves other queues alone.
  - `list-queues` is ordered by name.
  - `set/get-queue-policy` handle defaults, the unknown-key error and `cleanup_limit < 1`, and store intervals verbatim.
  - `spawn-task` creates a pending task and its first run with attempt 1.
  - An idempotency key returns the existing task with `created false`.
  - A nil params value is stored as JSON `null`.
  - `get-task-result` returns nil for an unknown task, and the state/result/failure-reason projection for known ones.

- [x] **Step 2: Run, expect failure**
  Run: the focused `absurd.sqlite.engine-test` command (see the top of Tasks).

- [x] **Step 3: Port** `create_queue`, `drop_queue`, `list_queues`, `get_queue_policy`, `set_queue_policy`, `spawn_task`, `get_task_result` per the porting table.

- [x] **Step 4: Run, expect pass**
  Run: the focused `absurd.sqlite.engine-test` command (see the top of Tasks).

- [x] **Step 5: Commit**
  `git commit -m "Port queue, spawn and task result procedures to SQLite"`

> Deviation: the tests and the port were written before the first run, so the "run, expect failure" step was skipped for this task.
> Deviation: every procedure takes `(conn now ...)`, including read-only ones that ignore `now` (the plan's uniform signature), so `SqliteStore` can wire them all the same way.
> Deviation: operations on a queue that does not exist (`spawn-task`, `get-task-result`) raise a `SQLException` with SQLState `42P01` (`Queue "x" does not exist`). Postgres fails there because the per-queue tables are missing; SQLite's shared tables would otherwise accept the rows silently. Task and run ids are parsed as UUIDs and lower-cased, like a `::uuid` cast, so a malformed id raises `22P02`.
> Deviation: a numeric underflow found by the Task 3 codex review (`"1e-999"`) is rejected like Postgres does; fixed in its own commit on top of Task 3.

### Task 5: Claims, run lifecycle, cancel and retry

**Files:**
- Modify: `src/absurd/sqlite/engine.clj`
- Test: `test/absurd/sqlite/engine_test.clj`

- [x] **Step 1: Write tests**
  - `claim-tasks` honours `qty` and `available_at`, sets `running`/`claimed_by`/`claim_expires_at`, and bumps the task's `attempts`/`first_started_at`.
  - An expired lease is failed with `$ClaimTimeout` and retried.
  - `max_delay` and `max_duration` cancellation are applied before claiming.
  - `complete-run` on a failed run raises AB002, and on a cancelled run raises AB001.
  - `fail-run` with exponential retry creates a sleeping run at the right `available_at`, and with `max_attempts` reached marks the task `failed`.
  - `schedule-run-after` sleeps the run relative to the engine clock.
  - `extend-claim` with 0 raises "extend_by must be > 0".
  - `cancel-task` is idempotent and a no-op on terminal tasks, and an unknown task raises "not found".
  - `retry-task` in place and with `spawn_new`.

- [x] **Step 2: Run, expect failure**
  Run: the focused `absurd.sqlite.engine-test` command (see the top of Tasks).

- [x] **Step 3: Port** `claim_task`, `complete_run`, `schedule_run`, `fail_run`, `extend_claim`, `cancel_task`, `retry_task`.

- [x] **Step 4: Run, expect pass**
  Run: the focused `absurd.sqlite.engine-test` command (see the top of Tasks). Then run `bb test` (Postgres only) to confirm nothing regressed.

- [x] **Step 5: Commit**
  `git commit -m "Port claim, run lifecycle, cancel and retry procedures to SQLite"`

> Deviation: the "run, expect failure" step was skipped again; the codex review of Task 4 (an explicit `retry_strategy: nil` must raise AB003 on spawn) was fixed in its own commit on top of Task 4.
> Deviation: besides `schedule-run` (takes the wake time, like the PL/pgSQL function), the engine has `schedule-run-after` (takes seconds) because that is what `Store` calls. Failed-lease sweeps write `claimExpiredAt` as `Instant#toString` (`2024-05-01T09:00:00Z`) rather than Postgres' `+00:00` form.

### Task 6: Checkpoints and events

**Files:**
- Modify: `src/absurd/sqlite/engine.clj`
- Test: `test/absurd/sqlite/engine_test.clj`

- [x] **Step 1: Write tests**
  - A checkpoint from a later attempt is not overwritten by an earlier attempt, and is hidden from `get-task-checkpoint-states` for an earlier run.
  - Checkpoint writes on a cancelled task raise AB001.
  - `extend_by` on a checkpoint write extends the claim.
  - `await-event` suspends and registers a wait, sleeping until infinity without a timeout.
  - `emit-event` wakes the waiter, writes the checkpoint and deletes the wait.
  - First emit wins.
  - An event emitted before the await resolves immediately.
  - A timed-out wait resumes once without re-registering.
  - A nil payload is stored as JSON `null`, distinct from the "not emitted" sentinel.

- [x] **Step 2: Run, expect failure**
  Run: the focused `absurd.sqlite.engine-test` command (see the top of Tasks).

- [x] **Step 3: Port** `set_task_checkpoint_state`, `get_task_checkpoint_state`, `get_task_checkpoint_states`, `await_event`, `emit_event`.

- [x] **Step 4: Run, expect pass**
  Run: the focused `absurd.sqlite.engine-test` command (see the top of Tasks).

- [x] **Step 5: Commit**
  `git commit -m "Port checkpoint and event procedures to SQLite"`

> Deviation: the Task 5 codex review (`max_duration` is a `bigint`, not an `integer`, so the 32-bit cast made `fail-run` throw for large values) was fixed in its own commit on top of Task 5.
> Deviation: engine functions are named after the PL/pgSQL procedures without a bang (`set-task-checkpoint-state`, and `set-queue-policy` renamed from `set-queue-policy!`); only `SqliteStore` methods carry the `Store` protocol's bang names. `emit-event` also checks that the queue exists (`42P01`), like `spawn-task` and `claim-tasks`.
> Deviation: `get-task-checkpoint-states` breaks `updated_at` ties by insertion order (`rowid`) so the order is deterministic under a pinned clock; Postgres leaves ties unordered.

### Task 7: SqliteStore and client wiring

**Files:**
- Create: `src/absurd/sqlite.clj`
- Modify: `src/absurd/core.clj`
- Test: `test/absurd/sqlite_test.clj`

- [x] **Step 1: Write tests**
  - `(absurd/client {:db "jdbc:sqlite:<tmp>"})` infers `:sqlite` and runs a task end to end.
  - A DataSource with `:backend :sqlite` works.
  - A single shared Connection works.
  - `:backend :sqlite` with nil `:db` throws `:absurd/invalid-db`.
  - `:backend :mysql` throws `:absurd/invalid-backend`.
  - The schema is created lazily on the first call, and the constructor doesn't touch the database (use a DataSource whose `getConnection` throws).
  - An app table in the same file is untouched.

- [x] **Step 2: Run, expect failure**
  Run: the focused command with `absurd.sqlite-test`.

- [x] **Step 3: Implement**
  - `SqliteStore [connectable lock now-fn schema]`. Each method forces `schema`, then runs the engine function inside `with-tx` with `now` read from `now-fn` (truncated to ms), wrapped in `sql/with-task-state-errors`.
  - `current-time` returns `(now-fn)` truncated to ms.
  - Client: backend inference and validation per the design. A SQLite URL string becomes `jdbc/get-datasource {:jdbcUrl url}`.

- [x] **Step 4: Run, expect pass**
  Run: the focused command with `absurd.sqlite-test`, then `bb test` (Postgres only).

- [x] **Step 5: Commit**
  `git commit -m "Add SQLite store and client backend selection"`

> Deviation: the schema guard is an atom flag instead of a `delay`. A `delay` caches a failed first run forever, so one transient `SQLITE_BUSY` at startup would break the store permanently; the flag retries on the next operation (`a-failed-first-operation-is-retried` covers it). The record fields are unchanged (`schema` now holds the atom).
> Deviation: `absurd.sqlite/store` takes `{:db-now-fn ...}` as its option (the same key as the client). The client map also gains `:backend`. `:backend :sqlite` rejects a string that is not a `jdbc:sqlite:` URL with `:absurd/invalid-db`.
> Note: no codex finding for Task 6 (the review found no issues).

### Task 8: Run the parity suite on SQLite

**Files:**
- Modify: `test/absurd/test_helpers.clj`, then whatever the failures point at (engine first, tests only where the difference is a documented divergence)

- [x] **Step 1: Enable the `sqlite` branch of `with-conn`**
  Temp file per test, a DataSource, `*db-now*` reset per test, files deleted afterwards. The SQLite row helpers query the `absurd_*` tables.

- [x] **Step 2: Run the whole suite on SQLite**
  Run: `ABSURD_TEST_BACKENDS=sqlite bb test`
  Expected: initially some failures.

- [x] **Step 3: Fix divergences**
  - Treat every failure as an engine bug unless it is a divergence the design documents (interval normalization, partitioned queues). Those get `h/only :postgres` plus a SQLite counterpart assertion.
  - Never loosen a parity assertion to make SQLite pass.

- [x] **Step 4: Run both backends**
  Run: `bb test`
  Expected: PASS on both backends.

- [x] **Step 5: Commit**
  `git commit -m "Run the parity suite against SQLite"`

> Result: the unmodified parity suite passed on SQLite on the first run (the engine tests had already pinned the Postgres semantics), so step 3 needed no engine fixes. I confirmed the run really used SQLite (`*backend*` is `:sqlite` and `*conn*` is a SQLite DataSource) and repeated it three times to check for flakiness. `ABSURD_TEST_BACKENDS` now defaults to `postgres,sqlite`.
> Result: the only Postgres-only guards (`h/only :postgres`) are `schema-test`'s version check, the Postgres URL tests in `client-test` and the partitioned/normalized-interval test. Their SQLite counterparts (schema version recorded, intervals stored verbatim and grammar enforced, partitioned queues rejected) are new tests in `client_test.clj` and `schema_test.clj`. No parity assertion was loosened.
> Deviation: the Task 7 codex review found two real issues, fixed in their own commit and re-reviewed clean: the engine clock was read before waiting for the locks (now read inside the transaction), and `jdbc:sqlite::memory:` URLs cannot work with a DataSource (now rejected with `:absurd/invalid-db`; pass a Connection instead).

### Task 9: SQLite-specific behaviour and stress test

**Files:**
- Modify: `test/absurd/sqlite_test.clj`

- [x] **Step 1: Write tests**
  - **Stress:** a file DataSource, `start-worker!` with `:concurrency 4`, and 200 spawned tasks, each recording its task id into a concurrent set and incrementing an `AtomicLong`. Afterwards the counter is 200, the set has 200 ids, and every task is `completed`.
  - **Steps under concurrency:** the same setup with a handler that runs step `a`, then step `b`, then throws when `(:attempt ctx)` is 1. The throw sits outside both steps, after both checkpoints commit. Every task completes on attempt 2, and each step body runs exactly once per task, because attempt 2 replays both steps from their checkpoints. A step whose body throws before its checkpoint commits must run again on retry. That case is already covered by `failed-step-does-not-save-checkpoint-and-reexecutes`.

- [x] **Step 2: Run, expect pass** (or fix the engine)
  Run: `ABSURD_TEST_BACKENDS=sqlite bb test`

- [x] **Step 3: Commit**
  `git commit -m "Add SQLite concurrency tests"`

> Result: both tests passed on the first run, and twice more under `bb test` (about 3-5 s each). Producers run on their own clients over the same database file (4 threads), so spawns contend for SQLite's write lock the way separate processes would, on top of the 4 worker threads sharing one store. Besides the counters, the tests assert 200 runs total (no task claimed twice) for the stress case and 400 runs / 400 checkpoints for the steps case.
> Note: the codex review of Task 8 found no issues.

### Task 10: Docs, changelog, backlog

**Files:**
- Modify: `README.md`, `CHANGELOG.md`, `docs/parity.md`
- Create: a backlog entry via /backlog

- [x] **Step 1: README**
  Add a "SQLite backend" section:
  - adding `org.xerial/sqlite-jdbc`
  - client examples (URL, DataSource with `:backend`)
  - `ensure-schema!`
  - the single-process recommendation
  - the divergences: no partitioned queues, policy intervals stored verbatim, no cleanup yet, not shared with other-language SDKs, engine clock is the JVM clock

  Mention `ABSURD_TEST_BACKENDS` in the development notes.

- [x] **Step 2: CHANGELOG and parity**
  Add the SQLite backend and the `absurd.store` protocol under Unreleased. In `docs/parity.md`, note that the suite runs on both backends and list the Postgres-only tests.

- [x] **Step 3: Backlog**
  Add "SQLite cleanup: enforce cleanup_ttl/cleanup_limit" with /backlog.

- [x] **Step 4: Full check**
  Run: `bb check && bb example-check`
  Expected: PASS

- [x] **Step 5: Commit**
  `git commit -m "Document the SQLite backend"`

> Note: the backlog entry (`docs/backlog/sqlite-cleanup.md`) is its own commit, as the backlog skill requires, and bootstrapped `AGENTS.md` (the repo had none) with the backlog convention. The README, CHANGELOG and `docs/parity.md` changes are in the "Document the SQLite backend" commit.
> Note: `bb check` ran green on both backends (fmt, lint, outdated, 309 tests, 1255 assertions) and `bb example-check` passed.

## Completion summary

**Implemented.** absurd-clj runs on SQLite behind the same public API. `absurd.store` defines a `Store` protocol (one method per stored procedure) with a `PostgresStore` over `absurd.sql`, and `absurd.core` reaches the database only through it. The SQLite backend is `absurd.sqlite` (`SqliteStore`, `store`, `ensure-schema!`), `absurd.sqlite.schema` (static `absurd_*` tables, WAL, schema version 1) and `absurd.sqlite.engine` (every Absurd procedure ported to Clojure over a `BEGIN IMMEDIATE`/`BEGIN` transaction with an in-process lock, plus UUIDv7, retry delay and JSON/time helpers). `absurd.core/client` gained `:backend` and `:db-now-fn` and infers SQLite from a `jdbc:sqlite:` URL. The test helpers are backend-aware, so the existing parity suite runs once per backend (`ABSURD_TEST_BACKENDS`, default `postgres,sqlite`) and passed on SQLite unmodified. New tests cover the engine per procedure group (`engine_test.clj`), client wiring and deployment shapes, and a 4-thread, 200-task stress test with steps under concurrency (`sqlite_test.clj`). Docs: README SQLite section, CHANGELOG, `docs/parity.md`, and a backlog entry for cleanup.

**Verification.** `bb check` (format, lint, outdated, tests on both backends: 309 tests, 1255 assertions, 0 failures) and `bb example-check` pass. End to end with a scratch project that depends on the library plus `sqlite-jdbc`: process 1 spawned a task that ran a step and suspended on `await-event`, process 1 exited, a fresh JVM saw the task as `:sleeping` in the same file, emitted the event, and its worker completed the task without re-running the step.

**Issues encountered.** The codex reviews found real defects, each fixed in its own commit on top of the task it came from: numeric underflow accepted in retry parameters (Task 3), an explicit `retry_strategy: nil` skipping validation (Task 4), `max_duration` cast to 32 bits instead of `bigint` (Task 5), the engine clock read before waiting for locks and `jdbc:sqlite::memory:` URLs that cannot work with a DataSource (Task 7), plus a blank `ABSURD_TEST_BACKENDS` silently running no tests (Task 2). Tasks 6, 8 and 9 reviewed clean. Codex itself failed twice on transient capacity errors and was re-run.

**Deviations (all in one place; each is also noted under its task).**

- Task 2: `ABSURD_TEST_BACKENDS` defaulted to `postgres` until Task 8 flipped it. `queue-policy-methods` was split into a backend-neutral test and a Postgres-only partitioned one (115 tests instead of 114). `delete-task!` deletes rows directly instead of calling `cleanup_tasks`. `fake-now-fn` takes no argument.
- Task 3: sqlite-jdbc pinned to `3.53.4.0` (latest). `absurd_queues` has `default_partition`. `ensure-schema!` takes the engine environment, and the public wrapper builds one. `with-tx` plus `with-read-tx` macros (and `.clj-kondo` `:lint-as`). `uuidv7` takes the engine clock and stays strictly increasing within the process.
- Task 4: every procedure takes `(conn now ...)`, including reads. Missing-queue operations raise SQLState `42P01`, and ids are parsed as UUIDs (`22P02` on garbage). "Run, expect failure" skipped for Tasks 4 and 5.
- Task 5: `schedule-run` plus `schedule-run-after`; `claimExpiredAt` is `Instant#toString`.
- Task 6: engine functions have no bang (`set-queue-policy` renamed); `emit-event` checks the queue exists; checkpoint-state ordering breaks `updated_at` ties by `rowid`.
- Task 7: the schema guard is an atom flag rather than a `delay` (a `delay` caches a failed first run forever); `store` takes `:db-now-fn`; the client map also has `:backend`; in-memory SQLite URLs are rejected with `:absurd/invalid-db`.
- Task 8: no engine fixes were needed; counterpart tests for the documented divergences were added.
- Task 10: `AGENTS.md` was created to bootstrap the backlog convention.

**What the plan could have specified better:** it fixed the structure and the tests well, but left the Postgres edge semantics the engine must mimic to the executor (errors for a missing queue, JSON null versus an absent option key, `bigint` versus `integer` casts, float underflow) and two SQLite-specific traps (reading the clock before the lock wait, in-memory URLs under a DataSource), and it prescribed a `delay` for the schema guard, which breaks retry after a transient failure; codex review caught all of these, not the plan.
