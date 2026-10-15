# SQLite Explicit Schema and Shipped Migration Implementation Plan

> **For agentic workers:** Use executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The SQLite backend stops creating its schema on the first operation; the app creates it explicitly with `absurd.sqlite/ensure-schema!` or by applying a SQL migration file the library ships on the classpath (usable with ragtime, migratus, Flyway, or by copying).

**Tech Stack:** Clojure 1.12, next.jdbc, org.xerial/sqlite-jdbc, ragtime.next-jdbc (test only), eftest via cloverage.

---

## Design

### Why

Today every `SqliteStore` operation goes through `run` → `ensure-ready!` (`src/absurd/sqlite.clj:31-47`), which creates the `absurd_*` tables once per store. Schema changes hidden inside a runtime call are surprising in production. Apps that manage their database with a migration tool also want the DDL as a file they can apply in order with their own migrations. The Postgres backend already never creates schema: users run `absurdctl init` (README "Setup"). This change makes SQLite work the same way.

Nothing is released yet (no git tags; SQLite sits under `Unreleased` in `CHANGELOG.md`), so removing lazy creation breaks no users.

### Shape

1. **The DDL moves to a resource file and becomes the single source of truth.**
   - `resources/absurd/sqlite/migrations/001-absurd-init.up.sql` holds every `create table if not exists` / `create index if not exists` statement now in the `statements` vector of `src/absurd/sqlite/schema.clj:19-120`, plus `insert or ignore into absurd_meta (key, value) values ('schema_version', '1')`.
   - `001-absurd-init.down.sql` drops the seven `absurd_*` tables (indexes go with them).
   - Statements end in `;` and are separated by a line holding only `--;;`. ragtime and migratus split on `--;;`; Flyway splits on `;` and reads `--;;` as a comment.
   - The file never contains `pragma journal_mode`: migration tools run each migration in a transaction (ragtime does by default, see `ragtime.next-jdbc/execute-sql!`), and SQLite cannot switch to WAL inside one.
   - The statements keep `if not exists`. The same file then serves the idempotent `ensure-schema!`, and an app that started with `ensure-schema!` can later adopt a migration tool: the file applies cleanly over existing tables.
   - `deps.edn` `:paths` becomes `["src" "resources"]` so the file ships in the jar.

2. **`absurd.sqlite.schema` reads that file.**
   - The `statements` vector is replaced by a private var loaded at namespace load from `(io/resource "absurd/sqlite/migrations/001-absurd-init.up.sql")`.
   - It is split with ragtime's exact regex, `#"(?m)\n\s*--\s?;;\s*\n"`, then trimmed, with blank chunks dropped. A missing resource fails loudly at load time.
   - `ensure-schema!` behaves as before: it sets WAL outside the transaction, executes the statements in one write transaction, then checks `schema_version` and throws `:absurd/schema-version-mismatch` on a mismatch. The separate `insert or ignore` in `create-tables!` goes away, because the file now does the insert.
   - `schema-version` stays `"1"`.
   - **Future versions (not built now):** schema v2 ships as a new `002-....up.sql` and never edits 001. `ensure-schema!` then becomes a small runner that applies the files above the stored version. Note this in the `schema.clj` ns docstring.

3. **No lazy creation.**
   - `ensure-ready!` and the `schema` field of `SqliteStore` go away. `run` calls `engine/run-tx` directly.
   - `store` builds `(->SqliteStore connectable lock now-fn)`.
   - The client constructor still never connects.

4. **Missing-schema error.**
   - Without the schema, the first operation fails inside the engine with sqlite-jdbc's `[SQLITE_ERROR] SQL error or missing database (no such table: absurd_queues)`.
   - `run` catches an `SQLException` whose message matches `#"no such table: absurd_"` and rethrows `(ex-info "The absurd SQLite schema is missing; call absurd.sqlite/ensure-schema! at boot or apply the migration absurd/sqlite/migrations/001-absurd-init.up.sql" {:type :absurd/schema-missing} e)`.
   - Other `SQLException`s pass through unchanged.
   - This costs nothing at runtime and does no implicit database work. No engine message contains that text: the engine's own "does not exist" errors use `42P01` with `Queue "x" does not exist`.
   - A lazy read-only version check was rejected: it would bring back hidden first-operation work, which this change removes.

5. **WAL for migration-tool users.**
   - `ensure-schema!` still switches the file to WAL.
   - Apps on a migration tool set it in the URL (`jdbc:sqlite:app.db?journal_mode=WAL`, a sqlite-jdbc connection pragma) or on their DataSource.
   - WAL is not required for correctness, since writes are serialized by `BEGIN IMMEDIATE` and the in-process lock. It is required for reads to run alongside a write. The README documents it; the library does not enforce it.

### Using the migration with ragtime

The ragtime migration ID is the file basename, `001-absurd-init`. An app combines the two sources:

```clojure
(ragtime.repl/migrate
  {:datastore (ragtime.next-jdbc/sql-database ds)
   :migrations (concat (ragtime.next-jdbc/load-resources "absurd/sqlite/migrations")
                       (ragtime.next-jdbc/load-resources "migrations"))})
```

`load-resources` reads classpath directories inside jars (through resauce). migratus uses only the numeric prefix as the ID, so `001` would clash with an app's own migration 1. migratus and Flyway (`V<n>__name.sql`) users copy the file into their migrations directory under their own name. The README says this.

### Testing

- The shared parity suite (`h/with-conn`) gets its schema from `ensure-schema!` in the `with-sqlite` fixture.
- `h/call-with-sqlite-db` stays a raw empty file: `engine_test.clj` relies on that.
- `sqlite_test.clj` gains a local `call-with-schema-db` for the tests that run tasks.
- New tests:
  - operations on an empty file raise `:absurd/schema-missing` and create no tables, and the constructor still never connects;
  - other SQLite errors are not mapped;
  - ragtime applies `001-absurd-init` from the classpath to a fresh file opened with `?journal_mode=WAL`, a task runs end to end, the URL put the file in WAL, and `rollback-last` drops every `absurd_*` table.
- Removed tests: `the-constructor-never-connects-and-the-schema-is-created-lazily` and `a-failed-first-operation-is-retried`. Both covered the lazy guard, which no longer exists.

## File Structure

| File | Change |
|---|---|
| `resources/absurd/sqlite/migrations/001-absurd-init.up.sql` | Create: the schema DDL plus the `schema_version` insert. |
| `resources/absurd/sqlite/migrations/001-absurd-init.down.sql` | Create: drop the seven `absurd_*` tables. |
| `deps.edn` | Modify: `:paths ["src" "resources"]`; add `dev.weavejester/ragtime.next-jdbc` to `:test` `:extra-deps`. |
| `src/absurd/sqlite/schema.clj` | Modify: load statements from the resource; drop the vector and the separate meta insert; update the ns docstring. |
| `src/absurd/sqlite.clj` | Modify: remove `ensure-ready!` and the `schema` field; map the missing-table error; update the ns and `ensure-schema!` docstrings. |
| `test/absurd/test_helpers.clj` | Modify: `with-sqlite` calls `sqlite/ensure-schema!` on the DataSource. |
| `test/absurd/sqlite_test.clj` | Modify: `call-with-schema-db` helper; replace the two lazy tests; add the missing-schema and ragtime tests. |
| `README.md`, `CHANGELOG.md`, `docs/parity.md` | Modify: document explicit schema creation, the shipped migration, WAL and `:absurd/schema-missing`. |

## Commands

- SQLite-only suite (no Docker): `ABSURD_TEST_BACKENDS=sqlite bb test`
- Full suite (needs Docker for Postgres): `bb test`
- Lint and format: `bb lint`, `bb fmt-check`
- If a dev nREPL is running (`.nrepl-port`), prefer /nrepl-eval to reload and run single namespaces.

### Task 1: Move the DDL into a shipped migration file

**Files:**
- Create: `resources/absurd/sqlite/migrations/001-absurd-init.up.sql`, `resources/absurd/sqlite/migrations/001-absurd-init.down.sql`
- Modify: `deps.edn`, `src/absurd/sqlite/schema.clj`
- Test: `test/absurd/sqlite/engine_test.clj` (existing schema tests)

- [x] **Step 1: Write the up migration.**
  - Copy every statement from the `statements` vector in `src/absurd/sqlite/schema.clj` verbatim. Expand the `run-states` check inline in both `absurd_tasks` and `absurd_runs`.
  - Add the final statement `insert or ignore into absurd_meta (key, value) values ('schema_version', '1')`.
  - Put a short header comment (what the file is, that it is the absurd-clj SQLite schema v1, and to set WAL on the connection) at the top of the *first* statement's chunk, never as a chunk of its own: ragtime would send a comment-only chunk to the driver as a statement.
  - Separate statements with a line holding only `--;;`, with a newline before and after.
  - End every statement with `;`. Flyway splits on semicolons and reads `--;;` as a comment, so the file needs both. sqlite-jdbc, ragtime and migratus all accept a trailing semicolon. Flyway is not tested here; this keeps the documented copy-the-file path valid.

- [x] **Step 2: Write the down migration.** `drop table if exists` for `absurd_waits`, `absurd_events`, `absurd_checkpoints`, `absurd_runs`, `absurd_tasks`, `absurd_queues`, `absurd_meta`, each ending in `;` and separated by `--;;` lines.

- [x] **Step 3: Ship resources.** In `deps.edn`, set `:paths ["src" "resources"]`.

- [x] **Step 4: Load the statements from the file.**
  - In `src/absurd/sqlite/schema.clj`, replace `run-states` and `statements` with:
    ```clojure
    (def migration-resource "absurd/sqlite/migrations/001-absurd-init.up.sql")
    ;; split like ragtime.sql/read-sql, so both read the file the same way
    (def ^:private statements
      (->> (str/split (slurp (io/resource migration-resource)) #"(?m)\n\s*--\s?;;\s*\n")
           (map str/trim)
           (remove str/blank?)
           vec))
    ```
  - Remove the `insert or ignore` from `create-tables!`; keep the version read and check.
  - Update the ns docstring: the DDL lives in the resource, apps may apply it with their own migration tool, and future versions ship as new numbered files.

- [x] **Step 5: Run the SQLite suite.** The existing `ensure-schema-is-idempotent-and-creates-every-table`, `ensure-schema-rejects-a-different-version` and `ensure-schema-enables-wal` in `engine_test.clj` cover the change.
  Run: `ABSURD_TEST_BACKENDS=sqlite bb test`
  Expected: all tests pass, the same count as before.

- [x] **Step 6: Commit.** `git commit -m "Ship the SQLite schema as a migration file"`

> Deviation (all tasks): `bb` is not installed here, so tests run as `ABSURD_TEST_BACKENDS=sqlite clojure -X:dev:test`.

### Task 2: Drop lazy schema creation and map the missing-schema error

**Files:**
- Modify: `src/absurd/sqlite.clj`, `test/absurd/test_helpers.clj`
- Test: `test/absurd/sqlite_test.clj`

- [x] **Step 1: Give the test fixtures an explicit schema.**
  - In `test/absurd/test_helpers.clj`, `with-sqlite` calls `(sqlite/ensure-schema! ds)` before binding. Require `absurd.sqlite`, and check there is no require cycle: `absurd.sqlite` doesn't require the test helpers.
  - In `sqlite_test.clj`, add a private `call-with-schema-db` that wraps `h/call-with-sqlite-db` and calls `sqlite/ensure-schema!` on the DataSource before `f`.
  - Switch every test that runs operations to it: end-to-end URL, DataSource, shared connection, clients sharing a connection, both clock tests, the errors test and both concurrency tests.
  - For the in-memory `Connection` case, call `(sqlite/ensure-schema! conn)` on the connection.
  - `tables-of-the-application-in-the-same-file-are-untouched` keeps `h/call-with-sqlite-db`: it creates its app table first, then calls `sqlite/ensure-schema!` explicitly before the client.
  - `ensure-schema-can-run-at-boot-and-is-repeatable` stays as is.

- [x] **Step 2: Write the failing tests.** Replace `the-constructor-never-connects-and-the-schema-is-created-lazily` and `a-failed-first-operation-is-retried` with:
  - `operations-need-an-explicit-schema`: on an empty file through `counting-datasource`:
    - the constructor makes zero `getConnection` calls;
    - `absurd/list-queues` and `absurd/spawn!` each throw `:absurd/schema-missing` (use `h/ex-type`), with an `SQLException` as `ex-cause`;
    - `(table-names ds)` is still empty afterwards;
    - after `sqlite/ensure-schema!`, `list-queues` returns `[]`.
  - Keep the existing "other engine errors stay SQLExceptions" assertion in `sqlite-errors-surface-like-the-postgres-ones`. It proves the mapping is narrow.

- [x] **Step 3: Run them to see them fail.**
  Run: `ABSURD_TEST_BACKENDS=sqlite bb test`
  Expected: `operations-need-an-explicit-schema` fails, because the store still creates the schema. Everything else passes.

- [x] **Step 4: Implement.** In `src/absurd/sqlite.clj`:
  - Delete `ensure-ready!` and the `schema` field (`(defrecord SqliteStore [connectable lock now-fn] ...)`, `(->SqliteStore connectable (engine/lock-for connectable) db-now-fn)`).
  - Wrap the body of `run` so that an `SQLException` whose `.getMessage` matches `#"no such table: absurd_"` is rethrown as the `:absurd/schema-missing` ex-info from the Design, with the original as cause. Any other exception is rethrown unchanged.
  - Update the ns docstring ("The schema is created on the first operation...") and the `ensure-schema!` docstring: the app creates the schema explicitly, by calling `ensure-schema!` at boot or by applying `absurd/sqlite/migrations/001-absurd-init.up.sql` with a migration tool. Operations on a database without it throw `:absurd/schema-missing`.
  - Fix the `sqlite_test.clj` ns docstring ("lazy schema" becomes "explicit schema").

- [x] **Step 5: Run the SQLite suite.**
  Run: `ABSURD_TEST_BACKENDS=sqlite bb test`
  Expected: all pass.

- [x] **Step 6: Commit.** `git commit -m "Require an explicit SQLite schema"`

> Deviation: `counting-datasource` lost its `failures` argument, since only the zero-failure case remains. The test registers a task before `spawn!`, which otherwise fails client-side with `:absurd/unregistered-task`.

### Task 3: Prove the migration works with ragtime

**Files:**
- Modify: `deps.edn`
- Test: `test/absurd/sqlite_test.clj`

- [x] **Step 1: Add the test dependency.** Add `dev.weavejester/ragtime.next-jdbc {:mvn/version "0.12.1"}` to `:test` `:extra-deps`. 0.12.1 is in `~/.m2`; use the latest release if antq reports a newer one.

- [x] **Step 2: Write the test** `the-shipped-migration-applies-with-ragtime`:
  - Use `h/call-with-sqlite-db`, but build a DataSource from `(str (url-of file) "?journal_mode=WAL")`.
  - Build `migrations` with `(ragtime.next-jdbc/load-resources "absurd/sqlite/migrations")`.
  - Assert the IDs are `["001-absurd-init"]`.
  - Run `(ragtime.core/migrate-all (ragtime.next-jdbc/sql-database ds) (ragtime.core/into-index migrations) migrations)`. Check the exact `ragtime.core` signatures in the jar before writing.
  - Assert that a client over that DataSource runs `run-one-task!` to `{:state :completed :result {:echo {:value 1}}}`.
  - Assert that `pragma journal_mode` returns `wal`. This proves the README's URL advice.
  - Run `ragtime.core/rollback-last` and assert that no `absurd_*` table remains. `ragtime_migrations` may remain.

- [x] **Step 3: Run it.**
  Run: `ABSURD_TEST_BACKENDS=sqlite bb test`
  Expected: passes.
  - If the WAL assertion fails, sqlite-jdbc does not take `journal_mode` from the URL. Find the supported form (`org.sqlite.SQLiteConfig` `setJournalMode`) and use it in both the test and the README.
  - If the migration fails, fix the file (a comment-only chunk, or a missing newline around `--;;`), not the loader.

- [x] **Step 4: Commit.** `git commit -m "Test the SQLite migration with ragtime"`

### Task 4: Documentation

**Files:**
- Modify: `README.md`, `CHANGELOG.md`, `docs/parity.md`

- [x] **Step 1: README SQLite section.** Replace the paragraph at `README.md:103-111` (schema "created on the first operation") with:
  - The app creates the schema explicitly, before the first operation. Operations on a database without it throw `:absurd/schema-missing`.
  - Option 1: `(sqlite/ensure-schema! datasource)` at boot. It creates the `absurd_*` tables when missing, switches the file to WAL, checks the schema version, and is safe to call on every start.
  - Option 2: a migration tool. The library ships `absurd/sqlite/migrations/001-absurd-init.up.sql` and `.down.sql` on the classpath. Include the ragtime snippet from the Design. migratus and Flyway users copy the file into their own migrations directory under their own name. Set WAL on the connection (`jdbc:sqlite:app.db?journal_mode=WAL`), because a migration cannot.
  - Future schema versions ship as new numbered files; 001 never changes.

  Add `:absurd/schema-missing` to the error table near `README.md:315`.

- [x] **Step 2: CHANGELOG.** The SQLite bullet is under `Unreleased`, so edit it in place rather than adding a "Changed" entry. Say:
  - the schema is created explicitly with `ensure-schema!` or the shipped migration;
  - operations without it throw `:absurd/schema-missing`.

- [x] **Step 3: docs/parity.md.** Line 30 lists "lazy schema" among the SQLite-specific tests. Change it to "explicit schema and the ragtime migration".

- [x] **Step 4: Commit.** `git commit -m "Document explicit SQLite schema creation"`

### Task 5: Full verification

- [x] **Step 1: Lint and format.** Run `bb fmt-check` and `bb lint`. Expected: no findings.

> Deviation: `cljfmt` is not installed here, so `bb fmt-check` was not run; `clj-kondo` reports no errors or warnings (one pre-existing info in `engine_test.clj:795`).
- [x] **Step 2: Full suite.** Run `bb test` (needs Docker). Expected: all pass on both backends. If Docker is unavailable, say so and report the SQLite-only result.
- [x] **Step 3: Examples compile.** Run `bb example-check`. Expected: `:ok` for both. They use Postgres, so this only checks nothing else broke.
- [x] **Step 4: The resource ships in the jar.** Run `clojure -T:build install :snapshot true` (or `bb install-snapshot`), then `unzip -l target/*.jar | grep absurd-init`. Expected: both `.sql` files listed under `absurd/sqlite/migrations/`. If slim does not copy `resources`, stop and report it rather than working around it.
- [x] **Step 5: Commit any fixes.** `git commit -m "Fix explicit SQLite schema review findings"`, only if something changed.

## Completion summary

**Status: completed.** The SQLite backend no longer creates its schema lazily.

- DDL moved to `resources/absurd/sqlite/migrations/001-absurd-init.{up,down}.sql`, loaded by `absurd.sqlite.schema` (split like ragtime) and shipped in the jar (checked in the installed snapshot jar).
- `SqliteStore` lost `ensure-ready!` and the `schema` field; a missing table raises `:absurd/schema-missing` with the `SQLException` as cause.
- New tests: `operations-need-an-explicit-schema` and `the-shipped-migration-applies-with-ragtime` (migrate, run a task, WAL via URL, rollback). Docs updated in README, CHANGELOG and `docs/parity.md`.
- Verification: full suite on both backends (Postgres via Docker), 314 tests, 1268 assertions, 0 failures; both examples compile; lint clean.
- Codex reviews: no must-fix findings. One P2 on Task 3 (`engine.clj` retry underflow) was a false positive. `number-text->double` already rejects `"1e-999"` (checked directly), so nothing was changed or backlogged.

**Deviations**
- `bb` and `cljfmt` are not installed here. Tests ran as `ABSURD_TEST_BACKENDS=sqlite clojure -X:dev:test` and `clojure -X:dev:test`; `bb fmt-check` was skipped.
- `counting-datasource` lost its `failures` argument, since only the zero-failure case remains.
- The missing-schema test registers a task before `spawn!`, which otherwise fails client-side with `:absurd/unregistered-task`.

**What the plan could have specified better:** the test commands (`bb` is not always available) and that `spawn!` needs a registered task in the missing-schema test.
