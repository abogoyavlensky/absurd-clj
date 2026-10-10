# absurd-clj SDK First Cut Implementation Plan

**Status: completed** (2026-10-09, branch `sdk-first-cut`)

> **For agentic workers:** Use executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first working Clojure SDK for Absurd (Postgres-native durable execution), feature-equivalent to the upstream Python sync client and verified by a ported parity test suite.

**Tech Stack:** Clojure 1.12, next.jdbc, org.postgresql JDBC driver, jsonista, tools.logging, Testcontainers (JDBC URL mode), clojure.test + eftest, test.check, docker compose for cross-SDK checks.

---

## Design

### Context

Absurd keeps all durable-execution logic in Postgres stored procedures (`absurd.*`). An SDK is a thin, language-idiomatic layer that calls those procedures and enforces a handful of conventions shared across SDKs. The upstream reference implementations live in a read-only clone at `/home/agent/.t3/scratch/2026-10-09-discuss-check-this-project-https-076ec978/absurd`:

- `sql/absurd.sql` — the stored procedures (main branch; the vendored copy must come from the `0.5.0` tag, which differs only in `get_schema_version()` returning `'0.5.0'`).
- `sdks/python/src/absurd_sdk/__init__.py` — the primary behavioural reference. Port the **sync** `Absurd` client and `TaskContext`; ignore `AsyncAbsurd`.
- `sdks/go/absurd/worker.go`, `context.go` — secondary reference for the worker loop and lease watchdog.
- `sdks/python/tests/*.py` — the parity test suite to port.
- `docs/sdks/python.md` — the documented API surface.

The library targets schema version `0.5.0` and is versioned `0.5.0-alpha`. No publishing, no tags.

### Namespaces

| Namespace | Responsibility |
|-----------|----------------|
| `absurd.json` | One JSON codec: encode Clojure data to a JSON string, decode with keywordized keys, `round-trip`, and `PGobject` construction for jsonb parameters. |
| `absurd.sql` | One function per stored procedure. Takes a next.jdbc `connectable` as first argument. Encodes jsonb params explicitly, decodes known jsonb columns by name, converts UUID columns to strings, and maps SQLSTATE `AB001`/`AB002` to signal exceptions. No business logic. |
| `absurd.core` | Public API: client construction, task registry, spawn and control-plane functions, task context operations, single-task execution, `work-batch!`, and the worker. |

No global `SettableParameter`/`ReadableColumn` protocol extensions. The SDK must not change how the host application's next.jdbc handles maps or `PGobject`.

### Client

`(absurd/client opts)` returns an open map:

```clojure
{:db          <javax.sql.DataSource | java.sql.Connection>  ; resolved connectable
 :owned-db?   boolean          ; true when the client opened it from a URL
 :queue       "default"
 :max-attempts 5
 :hooks       {:before-spawn (fn [task-name params opts] opts)
               :wrap-task-execution (fn [ctx execute] (execute))
               :on-lease-timeout (fn [ctx lease-seconds] nil)}
 :now-fn      (fn [] java.time.Instant)   ; default Instant/now; tests override
 :registry    (atom {})       ; task-name -> {:name :queue :max-attempts :cancellation :handler}
 :worker      (atom nil)}     ; running worker handle or nil
```

`:db` accepts a `javax.sql.DataSource`, a `java.sql.Connection`, a JDBC URL string, a `postgresql://` URL string (converted to `jdbc:postgresql://...`), or nil. Nil resolves to `ABSURD_DATABASE_URL`, then `PGDATABASE`, then `postgresql://localhost/absurd`. When the client receives a URL it builds a `next.jdbc/get-datasource` and sets `:owned-db? true`. `(absurd/close! app)` stops any worker and closes an owned `Connection`; a DataSource built from a URL has nothing to close.

Queue names are validated eagerly: non-empty and at most 57 UTF-8 bytes, otherwise `ex-info` with `{:type :absurd/invalid-queue-name}`.

### Public API

All names are in `absurd.core`. Options are maps with kebab-case keys; results are maps with kebab-case keys. Functions ending with `!` have side effects in the database.

Client-level:

```clojure
(client opts)
(close! app)
(register-task! app name handler) / (register-task! app name handler {:queue :max-attempts :cancellation})  ; returns app
(spawn! app task-name params) / (spawn! app task-name params {:queue :max-attempts :retry-strategy :headers :cancellation :idempotency-key})
;; => {:task-id "uuid" :run-id "uuid" :attempt 1 :created true}
(fetch-task-result app task-id) / (fetch-task-result app task-id {:queue})
;; => nil | {:state :completed :result v} | {:state :failed :failure v} | {:state :pending}...
(await-task-result app task-id {:timeout seconds :queue})
(emit-event! app event-name payload) / (emit-event! app event-name payload {:queue})
(cancel-task! app task-id {:queue})
(retry-task! app task-id {:max-attempts :spawn-new? :queue})   ; => {:task-id :run-id :attempt :created}
(create-queue! app name {:storage-mode :partition-lookahead :partition-lookback :cleanup-ttl :cleanup-limit :detach-mode :detach-min-age})
(drop-queue! app name) (list-queues app) (get-queue-policy app name) (set-queue-policy! app name opts)
(claim-tasks! app {:batch-size :claim-timeout :worker-id})
(work-batch! app {:worker-id :claim-timeout :batch-size})
(start-worker! app {:worker-id :claim-timeout :concurrency :batch-size :poll-interval})  ; => worker handle, non-blocking
(stop-worker! worker)   ; graceful: stop claiming, wait for in-flight tasks
```

Context operations (ctx first):

```clojure
(step ctx name f)                       ; f is zero-arg
(begin-step ctx name)                   ; => {:name :checkpoint-name :done? bool :state v}
(complete-step ctx handle value)
(sleep-for ctx step-name seconds)
(sleep-until ctx step-name instant-or-epoch-seconds)
(await-event ctx event-name) / (await-event ctx event-name {:timeout :step-name})
(await-task-result ctx task-id {:queue :timeout :step-name})
(heartbeat! ctx) / (heartbeat! ctx seconds)
(emit-event! ctx event-name payload)
(current-context)                       ; dynamic var *context*, nil outside a handler
```

`emit-event!` and `await-task-result` are dispatched on the first argument: a client map (has `:registry`) or a context map (has `::run-id`). Implement with a private `client?` predicate, not a protocol.

Enum-like options accept keywords or strings (`:exponential` or `"exponential"`, `:partitioned` or `"partitioned"`). They are converted with `name` at the boundary.

### Option normalization and spawn

`spawn!` builds the options map, passes it through `:before-spawn` if set (the hook returns the new options map), then resolves the queue and defaults exactly like Python `_prepare_spawn`:

- Registered task: queue is the registration queue; an explicit differing `:queue` is an error (`{:type :absurd/queue-mismatch}`).
- Unregistered task: `:queue` is required (`{:type :absurd/unregistered-task}`).
- `max-attempts` = explicit, else registration default, else client default.
- `cancellation` = explicit, else registration default.

The JSON options payload sent to `absurd.spawn_task` uses snake_case keys: `headers`, `max_attempts`, `retry_strategy` (`kind`, `base_seconds`, `factor`, `max_seconds`), `cancellation` (`max_duration`, `max_delay`), `idempotency_key`. Only present keys are emitted. Conversion is **not** recursive: a private `spawn-options->json` in `absurd.core` builds the payload from an explicit key list, converting only the SDK-defined keys above (top level, plus the known keys inside `retry-strategy` and `cancellation`). The `headers` value is passed through untouched, so a user header `:trace-id` is stored as `"trace-id"`, exactly as the user wrote it. The same explicit-key approach applies to queue policy options (`partition_lookahead`, `partition_lookback`, `cleanup_ttl`, `cleanup_limit`, `detach_mode`, `detach_min_age`) and retry-task options (`max_attempts`, `spawn_new`). User params, header contents, and step values are encoded as-is by `absurd.json`.

### Task context

Created per claimed task in `execute-task!`. It is a map:

```clojure
{:task-id "uuid" :run-id "uuid" :task-name "name" :attempt 1 :queue "q"
 :headers {:trace-id "..."}            ; decoded headers, keywordized, {} when null
 ::client app
 ::claim-timeout 120                   ; seconds
 ::wake-event (atom {:event "name" :payload v})   ; from the claim row, mutable
 ::checkpoints (atom {"name" value})   ; preloaded via get_task_checkpoint_states
 ::step-counter (atom {"name" 2})
 ::lease (atom {...})}                 ; watchdog state, nil in work-batch!
```

Checkpoint naming: a per-run counter; first use of a name is `name`, later uses `name#2`, `name#3`. Both `begin-step` and `sleep-*` and `await-event` consume a slot.

`begin-step` consults the cache, then `get_task_checkpoint_state`, caching the result. `complete-step` persists through `set_task_checkpoint_state` with `p_extend_claim_by` = claim timeout, caches, and returns the value. `step` = `begin-step` then either cached state or `(complete-step handle (f))`.

**Round-trip rule:** `complete-step` returns `(absurd.json/round-trip value)`, never the raw value. The cached entry is the round-tripped value. This guarantees attempt one and replay observe the same shape.

`sleep-until` stores the wake time as an ISO-8601 UTC string checkpoint (`java.time.Instant/toString` form, e.g. `2025-12-31T00:00:00Z`), on replay parses the stored string. If remaining seconds > 0 it calls `absurd.schedule_run(queue, run_id, absurd.current_time() + make_interval(secs => ?))` and throws the suspend signal. `sleep-for` computes wake time from `(:now-fn app)`.

`await-event` mirrors Python exactly: cached checkpoint wins; if the claim's `wake_event` equals the event name and `event_payload` is nil, clear it and throw the timeout error; otherwise call `absurd.await_event`; if `should_suspend` is false, cache and return payload, else throw suspend.

`await-task-result` on a context refuses `:queue` equal to the current queue (`{:type :absurd/same-queue-wait}`), then runs the poll loop inside a step named `$awaitTaskResult:<task-id>` (or `:step-name`), heartbeating every `max(500ms, claim-timeout/2)`. The stored checkpoint value is `{"state": "completed", "result": ...}` or `{"state": "failed", "failure": ...}` with the state as a string, byte-compatible with the Python SDK. After the step returns (first run or replay) the SDK converts `:state` back to a keyword with a private `snapshot<-json`, so client and context variants both return `{:state :completed ...}`.

Poll loop (shared by client and context variants): start delay 50 ms, double each poll up to 1000 ms, honour timeout (`{:type :absurd/timeout}`), error `{:type :absurd/task-not-found}` when the snapshot is nil.

### Signals and errors

All SDK exceptions are `ex-info` with a `:type` key:

| `:type` | Meaning | Where |
|---------|---------|-------|
| `:absurd/suspend` | Run suspended (sleep or event wait). Internal. | thrown by context ops, swallowed by `execute-task!` |
| `:absurd/cancelled` | SQLSTATE `AB001` | mapped in `absurd.sql` |
| `:absurd/failed-run` | SQLSTATE `AB002` | mapped in `absurd.sql` |
| `:absurd/timeout` | event or task-result wait timed out | user-visible |
| `:absurd/task-not-found` | task result lookup found nothing | user-visible |
| `:absurd/invalid-queue-name`, `:absurd/queue-mismatch`, `:absurd/unregistered-task`, `:absurd/same-queue-wait` | validation | user-visible |

`absurd.sql/with-task-state-errors` is a macro wrapping procedure calls that may raise `AB001`/`AB002`: it catches `org.postgresql.util.PSQLException`, reads `.getSQLState`, and rethrows as the matching signal with the original as cause. Any other exception propagates.

Failure payload written by `fail_run`: `{"name" <class simple name>, "message" <ex-message>, "traceback" <stack trace string>}`. For `ex-info` the name is `"ExceptionInfo"`. Use `clojure.stacktrace/print-cause-trace` captured into a string.

### Execution of one claimed task

Handlers are called as `(handler params ctx)`: decoded params first, context second, matching the Python SDK argument order.

`execute-task!` (private) follows Python `_execute_task` step by step:

1. Lookup registration. If missing: `schedule_run` with delay `15 + (hash of run-id mod 16)` seconds relative to `absurd.current_time()`, log at warn, return. If deferring fails, `fail_run` with the defer error, swallowing cancelled/failed-run signals.
2. If the registration queue differs from the client queue: `fail_run` with message `"Misconfigured task (queue mismatch)"`, return.
3. Build the context, preload checkpoints via `get_task_checkpoint_states`. This happens inside the same error boundary as the handler call: if header decoding or checkpoint loading throws, the run is failed through `fail_run` with that error (step 7), never left claimed.
4. Bind `*context*`, call the handler through `:wrap-task-execution` if set (`(hook ctx execute)` where `execute` is a zero-arg fn). The hook must return the result of `execute` and must not catch exceptions; document this.
5. On normal return: `complete_run` with the round-tripped result.
6. Catch `:absurd/suspend`, `:absurd/cancelled`, `:absurd/failed-run`: return silently.
7. Catch any other `Throwable`: `fail_run` with the failure payload, swallowing cancelled/failed-run from the `fail_run` call itself. Errors (not only Exceptions) are caught so a worker thread never dies.
8. In a `finally`: cancel the lease watchdog (see Worker) and restore `*context*`.

`work-batch!` claims up to `batch-size` tasks and runs them sequentially on the caller thread, used by tests and cron-style callers.

### Worker

`start-worker!` returns a handle map `{:executor ExecutorService :running? (atom true) :loop-thread Thread :in-flight (atom #{})}` that also implements `java.io.Closeable` through `reify` wrapping the map (expose the map under `:handle` or return a `reify` whose `close` calls `stop-worker!`). Preferred: return a record-free `reify java.io.Closeable clojure.lang.IDeref` where `deref` yields the state map, so `(with-open [w (start-worker! app opts)] ...)` works and tests can inspect state.

Loop (on a dedicated thread named `absurd-worker-<worker-id>`):

- Fixed thread pool of `concurrency` threads (`Executors/newFixedThreadPool`) with daemon threads named `absurd-task-<n>`.
- While running: compute free capacity; if none, wait `poll-interval` for a completion; else claim `min(batch-size, free)` tasks; if none, sleep `poll-interval`; else submit each to the pool. Claim errors are logged and followed by a `poll-interval` sleep, matching Go `RunWorker`.
- `stop-worker!` flips `running?`, joins the loop thread, calls `shutdown` on the pool and awaits termination (bounded, default 60 s, configurable `:shutdown-timeout`). In-flight tasks finish; nothing is force-cancelled. Returns `true` when the pool drained, `false` when the timeout expired with tasks still running; in the latter case it logs a warning with the count and leaves the tasks running. It never throws, and calling it twice is a no-op.
- Default `worker-id` is `<hostname>:<pid>`. Default `batch-size` is `concurrency`. Default `poll-interval` 250 ms, `claim-timeout` 120 s.

Lease watchdog: per executing task, a `ScheduledExecutorService` (one shared, single daemon thread) schedules a warning at `claim-timeout` seconds. The `::lease` atom holds `{:future ScheduledFuture :epoch n :stopped? bool}`. Every heartbeat and checkpoint write reschedules: cancel the current future, increment `:epoch`, schedule a new one capturing the new epoch. The reschedule delay is the duration actually sent to the database: `(heartbeat! ctx 300)` reschedules for 300 s, `(heartbeat! ctx)` and checkpoint writes use the claim timeout. The callback checks that the lease is not stopped and that its captured epoch is still current before doing anything, so a stale callback that was already running when a reschedule happened is a no-op (same guard as Go's `leaseWatchdog`). `execute-task!` stops the watchdog in its `finally` (set `:stopped? true`, cancel the future), so no warning or hook fires after a task completed, failed, or suspended. On firing it logs a warning and calls `:on-lease-timeout` with `(ctx seconds)` if set. Never terminates the process. `work-batch!` runs without a watchdog (`::lease` nil) and the reschedule helpers tolerate that.

Logging via `clojure.tools.logging` only; no logging backend dependency in the library.

### JSON codec

`absurd.json`:

- `mapper`: jsonista object mapper with `:decode-key-fn keyword` and `:encode-key-fn name`.
- `(write-str v)`, `(read-str s)`, `(round-trip v)` = `(read-str (write-str v))`.
- `(->jsonb v)` returns a `PGobject` of type `jsonb`; `(->jsonb nil)` returns a `PGobject` with value `"null"` so SQL `null` versus JSON `null` matches the Python SDK (`json.dumps(None)` = `"null"`).
- `(<-jsonb v)` accepts `PGobject`, `String`, or nil and returns decoded data.

Keyword keys decode as keywords; keyword *values* become strings after round-trip, and that is the documented behaviour. Sets become vectors. Instants must be converted by the user. This is intentional and covered by the determinism test.

### Database access

`absurd.sql` functions take `conn` (anything `next.jdbc` accepts) and use `jdbc/execute-one!`/`execute!` with `{:builder-fn rs/as-unqualified-kebab-maps}`. UUID columns come back as `java.util.UUID` and are converted to strings with `str`. UUID parameters are passed as strings with `::uuid` casts in SQL (`?::uuid`). Interval columns in `get_queue_policy` are returned as strings using `?::text` casts in the select list so the API is driver-neutral.

Every call that needs the same session (tests with `fake_now`, `work-batch!`) works because callers pass a `Connection`; when `:db` is a DataSource, each call gets its own pooled connection, which is correct for production use.

### Testing strategy

Three layers, all under `test/`:

1. **Parity suite** (`test/absurd/*_test.clj`): one Clojure test per Python sync test, same names translated to kebab-case, same assertions by reading `absurd.t_<queue>` and `absurd.r_<queue>` directly. Async-only Python tests and `run_step` decorator tests are skipped and listed in `docs/parity.md` with the reason.
2. **Replay determinism** (`test/absurd/determinism_test.clj`): test.check property with a generator for JSON-shaped Clojure values (nil, booleans, numbers, strings, keywords as values, vectors, maps with keyword and string keys, nested): a task runs `step` on attempt one, throws, and on attempt two the step returns a value equal to attempt one's return value.
3. **Cross-SDK** (`cross-sdk/`): docker compose with Postgres 16 and a `python:3.12` worker running `absurd-sdk==0.5.0`. Run by `bb cross-sdk-test`, in its own CI job, not part of `bb test`.

Test infrastructure (`test/absurd/test_helpers.clj`):

- `jdbc-url`: `jdbc:tc:postgresql:16-alpine:///absurd?TC_INITSCRIPT=file:test/resources/absurd.sql&TC_DAEMON=true`. Testcontainers starts one container per JVM for this URL and applies the schema once.
- `with-conn` fixture: opens one `java.sql.Connection` via `jdbc/get-connection`, sets `set timezone to 'UTC'`, binds `*conn*`, resets `absurd.fake_now` in `finally`, closes.
- `(set-fake-now! conn instant-or-nil)`: `set absurd.fake_now = '<iso>'` or `set absurd.fake_now = default`. Instants are formatted with `DateTimeFormatter/ISO_INSTANT`.
- `(test-client opts)`: `absurd/client` with `:db *conn*`, a fresh `sdk_<8 hex>` queue, and `:now-fn` that reads `current_setting('absurd.fake_now', true)` and falls back to `Instant/now`, mirroring Python's patched `_get_current_time`. Creates the queue.
- `get-task`, `get-run`, `get-runs`, `get-checkpoints`, `get-failure` helpers that select from the per-queue tables.

Vendored schema: `test/resources/absurd.sql` fetched from `https://raw.githubusercontent.com/earendil-works/absurd/0.5.0/sql/absurd.sql`. `bb sync-schema` (argument: tag, default from `deps.edn` version prefix `0.5.0`) refreshes it. A test asserts `absurd.get_schema_version()` returns `"0.5.0"`.

Cloverage and eftest stay as the runner (`bb test`). Container startup is ~5 s and happens once.

### Out of scope for this cut

Async client, `run_step` decorator analogue, typed task wrapper, virtual-thread executor option, publishing, Habitat, absurdctl.

---

## File Structure

```
absurd-clj/
├── deps.edn                              # modify: add runtime and test deps
├── bb.edn                                # modify: add sync-schema, cross-sdk-test tasks
├── .github/workflows/checks.yaml         # modify: add cross-sdk job
├── README.md                             # modify: usage docs
├── CHANGELOG.md                          # modify
├── docs/parity.md                        # create: Python test -> Clojure test mapping and skips
├── src/absurd/json.clj                   # create: codec
├── src/absurd/sql.clj                    # create: stored procedure wrappers, signal mapping
├── src/absurd/core.clj                   # create: client, context, execution, worker
├── src/absurd_clj/core.clj               # delete (scaffold placeholder)
├── test/absurd_clj/core_test.clj         # delete (scaffold placeholder)
├── test/resources/absurd.sql             # create: vendored schema 0.5.0
├── test/absurd/test_helpers.clj          # create
├── test/absurd/json_test.clj             # create
├── test/absurd/sql_test.clj              # create
├── test/absurd/client_test.clj           # create: connection defaults, queue validation, queue policy
├── test/absurd/spawn_test.clj            # create: spawn options, idempotency, retry-task
├── test/absurd/step_test.clj             # create
├── test/absurd/sleep_test.clj            # create
├── test/absurd/events_test.clj           # create
├── test/absurd/retry_test.clj            # create: backoff, cancellation
├── test/absurd/heartbeat_test.clj        # create
├── test/absurd/execution_test.clj        # create: unknown task, queue mismatch, traceback, hooks
├── test/absurd/task_result_test.clj      # create
├── test/absurd/worker_test.clj           # create
├── test/absurd/determinism_test.clj      # create
├── cross-sdk/compose.yaml                # create
├── cross-sdk/python/worker.py            # create
├── cross-sdk/python/client.py            # create
└── cross-sdk/run.sh                      # create
```

`src/absurd/core.clj` is expected to be 500–700 lines. If it crosses 800, split context operations into `src/absurd/context.clj` keeping public names re-exported from `absurd.core`; do not split pre-emptively.

---

## Tasks

### Task 1: Dependencies, vendored schema, test infrastructure

**Files:**
- Modify: `deps.edn`, `bb.edn`, `.gitignore`
- Create: `test/resources/absurd.sql`, `test/absurd/test_helpers.clj`, `test/absurd/schema_test.clj`
- Delete: `src/absurd_clj/core.clj`, `test/absurd_clj/core_test.clj`

- [x] **Step 1: Add dependencies**
  In `deps.edn` `:deps` add `com.github.seancorfield/next.jdbc {:mvn/version "1.3.1093"}`, `org.postgresql/postgresql {:mvn/version "42.7.7"}`, `metosin/jsonista {:mvn/version "0.3.13"}`, `org.clojure/tools.logging {:mvn/version "1.3.0"}`. In `:test` `:extra-deps` add `org.testcontainers/postgresql {:mvn/version "1.21.3"}`, `org.clojure/test.check {:mvn/version "1.1.1"}`, `ch.qos.logback/logback-classic {:mvn/version "1.5.18"}` (test-only logging backend). Add `"test/resources"` to `:test` `:extra-paths`. Run `bb outdated-check` afterwards and bump to whatever it reports as latest.

- [x] **Step 2: Vendor the schema and add `bb sync-schema`**
  Add a `sync-schema` task to `bb.edn` that takes an optional tag (default `"0.5.0"`) and downloads `https://raw.githubusercontent.com/earendil-works/absurd/<tag>/sql/absurd.sql` to `test/resources/absurd.sql` using `babashka.curl` or `babashka.http-client`. Run `bb sync-schema`.
  Expected: file exists, `grep "select '0.5.0'::text" test/resources/absurd.sql` matches.

- [x] **Step 3: Delete scaffold placeholders**
  Remove `src/absurd_clj/` and `test/absurd_clj/`. Update `dev/user.clj` test regex to `#"absurd.*-test"`.

- [x] **Step 4: Write `test/absurd/test_helpers.clj`**
  Implement `jdbc-url`, `*conn*`, `with-conn` fixture, `set-fake-now!`, `random-queue`, `test-client`, `get-task`, `get-run`, `get-runs`, `get-checkpoints`, `get-failure`, `sleep-ms`, per the design. Table-read helpers build SQL with `(str "absurd.t_" queue)`; queue names are generated by tests so interpolation is safe. Use `rs/as-unqualified-kebab-maps`. The `:now-fn` for test clients reads `select current_setting('absurd.fake_now', true) as v`; when non-blank parse as `Instant`, else `Instant/now`.

- [x] **Step 5: Write `test/absurd/schema_test.clj`**
  One test using `with-conn`: `select absurd.get_schema_version() as v` returns `"0.5.0"`.

- [x] **Step 6: Run tests**
  Run: `bb test`
  Expected: container starts, 1 test passes. Note the startup time in the task log.

- [x] **Step 7: Lint and format, commit**
  Run: `bb fmt && bb lint-init && bb lint`
  `git commit -m "Add deps, vendored schema 0.5.0 and testcontainers test harness"`

> Deviation: Testcontainers `org.testcontainers/testcontainers-postgresql 2.0.5` instead of `postgresql 1.21.3`. The 1.x line hardcodes Docker API 1.32, which the Docker 29 daemon rejects ("client version 1.32 is too old"); 2.0.5 connects fine and bundles the JDBC URL driver.
> Deviation: `test-client` is deferred to Task 4 because `absurd.core` does not exist yet; the helpers decode jsonb with jsonista directly so they do not depend on `absurd.json`.
> Deviation: Task 1 was verified by running eftest directly, since cloverage refuses to start with an empty `src/`. `bb test` is usable again from Task 2.
> Deviation: added `test/resources/logback-test.xml` to keep Testcontainers logging at WARN, and `.tmp/` to `.gitignore`.

### Task 2: JSON codec

**Files:**
- Create: `src/absurd/json.clj`, `test/absurd/json_test.clj`

- [x] **Step 1: Write tests**
  Cover: `write-str` of `{:a-b 1}` yields `{"a-b":1}` (no case conversion); `read-str` keywordizes keys; `round-trip` turns keyword values into strings and sets into vectors; nested maps and vectors survive; `->jsonb nil` has value `"null"` and type `jsonb`; `<-jsonb` handles `PGobject`, `String`, nil; numbers decode as `Long`/`Double`.

- [x] **Step 2: Run tests, expect failure**
  Run: `clojure -X:dev:test` (or `bb test`)
  Expected: FAIL (namespace missing)

- [x] **Step 3: Implement `absurd.json`**
  Per the design. Keep it under 40 lines.

- [x] **Step 4: Run tests, expect pass**
  Run: `bb test`
  Expected: PASS

- [x] **Step 5: Commit**
  `git commit -m "Add JSON codec with jsonb helpers"`

> Deviation: the mapper enables Jackson `USE_LONG_FOR_INTS` so integers decode as `Long` (jsonista defaults to `Integer` for small values).

### Task 3: Stored procedure wrappers

**Files:**
- Create: `src/absurd/sql.clj`, `test/absurd/sql_test.clj`

- [x] **Step 1: Write tests**
  Integration tests with `with-conn` and a random queue, calling the wrappers directly: `create-queue!` + `list-queues` includes it; `spawn-task!` returns string ids and `:created true`; `claim-task!` returns a vector of claim maps with `:params` decoded and keywordized and `:headers` nil or map; `set-task-checkpoint-state!` then `get-task-checkpoint-states` returns the decoded state; `complete-run!` on an already-failed run throws `ex-info` with `:type :absurd/failed-run` (set up by calling `fail-run!` twice? No: fail then complete); `cancel-task!` then `set-task-checkpoint-state!` throws `:absurd/cancelled`; `get-task-result` returns nil for unknown id; `get-queue-policy` returns intervals as strings.

- [x] **Step 2: Run tests, expect failure**
  Run: `bb test`
  Expected: FAIL

- [x] **Step 3: Implement `absurd.sql`**
  One function per procedure, signatures:

  ```clojure
  (create-queue! conn queue storage-mode)      ; storage-mode nil => 1-arg SQL overload
  (drop-queue! conn queue) (list-queues conn)  ; => vector of strings
  (get-queue-policy conn queue)                ; => map or nil, intervals as strings
  (set-queue-policy! conn queue policy-map)    ; policy already snake_case-keyed
  (spawn-task! conn queue task-name params options-map)   ; => {:task-id :run-id :attempt :created}
  (claim-tasks! conn queue worker-id claim-timeout qty)   ; => [{:run-id :task-id :attempt :task-name :params :retry-strategy :max-attempts :headers :wake-event :event-payload}]
  (complete-run! conn queue run-id result)
  (fail-run! conn queue run-id failure-map)
  (schedule-run-after! conn queue run-id seconds)        ; uses absurd.current_time() + make_interval
  (retry-task! conn queue task-id options-map)
  (set-task-checkpoint-state! conn queue task-id name value run-id extend-by)
  (get-task-checkpoint-state conn queue task-id name)    ; => {:checkpoint-name :state ...} or nil
  (get-task-checkpoint-states conn queue task-id run-id) ; => vector
  (await-event conn queue task-id run-id step-name event-name timeout) ; => {:should-suspend :payload}
  (emit-event! conn queue event-name payload)
  (extend-claim! conn queue run-id seconds)
  (cancel-task! conn queue task-id)
  (get-task-result conn queue task-id)   ; => {:task-id :state :result :failure-reason} or nil
  (current-time conn)                    ; => Instant from absurd.current_time()
  ```

  Plus the `with-task-state-errors` macro. Option maps arrive already snake_case-keyed (strings) from `absurd.core`; `absurd.sql` only encodes them. Wrap `complete-run!`, `fail-run!`, `set-task-checkpoint-state!`, `extend-claim!`, `await-event` with the macro, matching the Python call sites.

- [x] **Step 4: Run tests, expect pass**
  Run: `bb test`
  Expected: PASS

- [x] **Step 5: Commit**
  `git commit -m "Add stored procedure wrappers with signal mapping"`

> Deviation: `with-task-state-errors` catches `java.sql.SQLException` (the PSQLException superclass) and checks `getSQLState`, so it also works through driver wrappers.

### Task 4: Client construction, queue management, spawn and control plane

**Files:**
- Create: `src/absurd/core.clj`, `test/absurd/client_test.clj`, `test/absurd/spawn_test.clj`

- [x] **Step 1: Write client tests**
  Port: `test_sync_constructor_fails_before_connect_on_empty_queue`, `test_sync_queue_validation_is_permissive_but_bounded`, `test_sync_absurd_prefers_absurd_database_url_over_pgdatabase`, `test_sync_absurd_falls_back_to_pgdatabase`, `test_async_absurd_falls_back_to_default_absurd_uri` (as sync). Connection-default tests only check the resolved URL string via a public `resolve-database-url` function given an env map, so they need no database. `test_sync_queue_policy_methods` ported as-is.

- [x] **Step 2: Write spawn tests**
  Port all sync tests from `test_idempotency.py`, `test_retry_task.py`, and the Go `TestSpawnOptionsParity` (spawn with every option and assert the stored `headers`, `retry_strategy`, `max_attempts`, `cancellation` columns), `TestUnknownTaskRequiresExplicitQueueAtSpawn`, `TestDefaultCancellationApplied`, plus `before-spawn` hook tests from `test_hooks.py` that do not need execution (hook injects headers; hook receives task name and params).

- [x] **Step 3: Run tests, expect failure**
  Run: `bb test`
  Expected: FAIL

- [x] **Step 4: Implement client, queue management, spawn, control plane**
  In `absurd.core`: `resolve-database-url`, `->connectable` (DataSource, Connection, string, nil), `validate-queue-name`, `client`, `close!`, `register-task!`, `prepare-spawn` (private, mirrors Python), `spawn-options->json`, `queue-policy->json`, `retry-options->json` (private, explicit key lists, headers passed through untouched; add a test that a header key `:trace-id` is stored as `"trace-id"`), `spawn!`, `fetch-task-result`, `await-task-result` (client arity) with the shared poll loop, `emit-event!`, `cancel-task!`, `retry-task!`, `create-queue!`, `drop-queue!`, `list-queues`, `get-queue-policy`, `set-queue-policy!`, `claim-tasks!`. `create-queue!` creates then calls `set-queue-policy!` only when policy keys are present, like Python. Leave context and execution for Task 5; `emit-event!` dispatch on ctx can be added then.

- [x] **Step 5: Run tests, expect pass**
  Run: `bb test`
  Expected: PASS

- [x] **Step 6: Commit**
  `git commit -m "Add client, queue management, spawn and control plane"`

> Deviation: tests that need task execution (`retry_task` tests, `idempotency_key_persists_after_task_completes`, `idempotent_spawn_executes_only_once`, hook tests that read `ctx.headers` inside a handler) are ported in Task 5 once `work-batch!` exists. Task 4 covers the remaining spawn, idempotency, option-serialization, hook and control-plane tests.
> Deviation: `get-queue-policy` keywordizes `:storage-mode` and `:detach-mode` for consistency with `:state`; intervals stay strings. `create-queue!` rejects unknown storage modes with `:absurd/invalid-storage-mode`.
> Deviation: added `->jdbc-url`, which converts `postgresql://user:pw@host/db` URLs and bare database names into JDBC URLs, since pgjdbc does not accept libpq-style URLs.

### Task 5: Task context, steps, and single-task execution

**Files:**
- Modify: `src/absurd/core.clj`
- Create: `test/absurd/step_test.clj`, `test/absurd/execution_test.clj`

- [x] **Step 1: Write step tests**
  Port from `test_step.py`: `step_executes_and_returns_value`, `step_result_is_cached_and_not_reexecuted_on_retry`, `begin_complete_step_result_is_cached_and_not_reexecuted_on_retry`, `multistep_task_only_reexecutes_uncompleted_steps`, `repeated_step_names_work_correctly`, `repeated_begin_complete_step_names_auto_number_correctly`, `failed_step_does_not_save_checkpoint_and_reexecutes`. From `test_task_context.py`: `step_persists_and_reuses_checkpoints`. Add one Clojure-specific test: a step returning `{:k :v #{1}}` returns `{:k "v" [1]}` on attempt one.

- [x] **Step 2: Write execution tests**
  Port: `test_unknown_task_is_deferred_not_failed`, `test_unknown_task_defer_failure_preserves_error_context`, Go `TestQueueMismatchFailsClaimedRunImmediately`, Go `TestFailureIncludesTraceback` (assert `name`, `message`, `traceback` keys and that traceback contains the handler namespace), Go `TestInvalidHeadersFailRun` (headers that are not a JSON object fail the run: insert such a task row via SQL), `test_sync_worker_processes_task`. From `test_hooks.py`: the `wrap_task_execution` tests (hook sees ctx headers, hook result is the stored result, hook that throws fails the run), and `get_current_context` returns the ctx inside a handler and nil outside. Also assert that a handler throwing `java.lang.AssertionError` (an `Error`) still produces a failed run.

- [x] **Step 3: Run tests, expect failure**
  Run: `bb test`
  Expected: FAIL

- [x] **Step 4: Implement context and execution**
  `*context*` dynamic var, `current-context`, `make-context` (private, loads checkpoints), `checkpoint-name!` (private counter), `begin-step`, `complete-step`, `step`, `heartbeat!`, `emit-event!` ctx arity, `failure-payload` (private), `execute-task!` (private), `work-batch!`. Invalid headers (non-object JSON) are detected in `make-context` and fail the run with message `"invalid task headers"`.

- [x] **Step 5: Run tests, expect pass**
  Run: `bb test`
  Expected: PASS

- [x] **Step 6: Commit**
  `git commit -m "Add task context, steps and single-task execution"`

> Deviation: the execution-dependent tests deferred from Task 4 (retry-task, idempotency with execution, hook header capture) live in `execution_test.clj`. The `unknown_task_defer_failure` port redefines the private `unknown-task-defer-seconds` via `with-redefs-fn` + `ns-resolve` to force a `ClassCastException`, since Clojure has no equivalent of monkeypatching a module constant into a string.
> Deviation: `step-persists-and-reuses-checkpoints` reaches the private `make-context` through `ns-resolve`, mirroring the Python test's use of `_create_task_context`.
> Codex review (noted, not changed): (1) an explicit step name `x#2` can collide with the generated name of a repeated `x`; (2) the cache-miss fallback uses `get_task_checkpoint_state`, which is not run-scoped. Both match the Python and TypeScript SDKs byte for byte, and checkpoint naming must stay compatible for mixed-language workers, so the behaviour is kept and documented in the README instead.

### Task 6: Sleep, events, heartbeat, task-result waits

**Files:**
- Modify: `src/absurd/core.clj`
- Create: `test/absurd/sleep_test.clj`, `test/absurd/events_test.clj`, `test/absurd/heartbeat_test.clj`, `test/absurd/task_result_test.clj`

- [x] **Step 1: Write sleep tests**
  Port: `test_sleep_for_suspends_until_duration_elapses`, `test_sleep_for_schedules_relative_to_database_clock` (set `fake_now`, assert `available_at` on the run row equals fake now plus the duration), `test_sleep_until_checkpoint_prevents_rescheduling`, `test_sleep_for_schedules_future_run`, `test_sync_worker_suspends_until_alarm`. `sleep-until` accepts `Instant`, `java.util.Date`, or a number of epoch seconds.

- [x] **Step 2: Write events tests**
  Port all six from `test_events.py` and `test_await_event_flow` from `test_task_context.py`.

- [x] **Step 3: Write heartbeat tests**
  Port the sync tests from `test_heartbeat.py`: zero rejected by SQL (expect a `PSQLException`, not a signal), heartbeat on failed run throws `:absurd/failed-run`, `execute-task!` swallows failed-run on checkpoint and cancelled on complete/fail.

- [x] **Step 4: Write task-result tests**
  Port: `fetch_task_result_returns_none_for_unknown_task`, `fetch_task_result_tracks_lifecycle`, `await_task_result_times_out`, `task_context_await_task_result_waits_for_terminal_state` (two queues, second worked with `work-batch!` from a second client on the same connection), `task_context_await_task_result_rejects_same_queue`, Go `TestTaskContextAwaitTaskResultCheckpointSurvivesChildCleanupOnRetry`, Go `TestAwaitTaskResultNegativeTimeoutTimesOutImmediately`, Go `TestAwaitEventNegativeTimeoutDoesNotSleepForever`.

- [x] **Step 5: Run tests, expect failure**
  Run: `bb test`
  Expected: FAIL

- [x] **Step 6: Implement**
  `sleep-until`, `sleep-for`, `await-event`, `await-task-result` ctx arity with heartbeating, `poll-task-result` (private shared loop), `snapshot->json` / `snapshot<-json` (private; state keyword to string and back). The ctx variant's heartbeat uses `(::claim-timeout ctx)`. Add an assertion in `task_context_await_task_result_waits_for_terminal_state` that the returned `:state` is the keyword `:completed` on both the first run and the replay, and that the stored checkpoint has the string `"completed"`.

- [x] **Step 7: Run tests, expect pass**
  Run: `bb test`
  Expected: PASS

- [x] **Step 8: Commit**
  `git commit -m "Add sleep, events, heartbeat and durable task-result waits"`

> Deviation: `await-event` clamps a negative `:timeout` to 0 before calling the database, because `absurd.await_event` raises on negative values. This matches the Go SDK's observable behaviour (negative timeout wakes immediately).
> Deviation: `fetch_task_result_returns_none_for_unknown_task`, `await_task_result_times_out` and the negative task-result timeout are covered in `spawn_test.clj` rather than `task_result_test.clj`; a `fetch-task-result-reports-failures` test was added.
> Deviation: `sleep-until` also accepts an ISO-8601 string, and a wake time already in the past returns nil without suspending (same as upstream).
> Codex review: (noted, not changed) an event timeout is not checkpointed, so a handler that catches it and then suspends elsewhere re-registers the wait on replay. This matches the Python SDK, and storing a timeout marker under the event checkpoint would be read as a payload by other SDKs. (fixed) the context-level `await-task-result` now refreshes the lease when the wait starts and caps poll sleeps at half the heartbeat interval, so short claim timeouts cannot lapse between heartbeats; covered by `task-context-await-task-result-keeps-lease-alive-under-competing-claims`.

### Task 7: Retry and cancellation parity

**Files:**
- Create: `test/absurd/retry_test.clj`

- [x] **Step 1: Write tests**
  Port all fourteen tests from `test_retry.py` and `test_registered_task_without_retry_default_uses_client_default`. These exercise the SQL through the SDK, so no new implementation is expected; they lock in option serialization and signal handling. `test_cancel_non_existent_task_errors` expects a `PSQLException`.

- [x] **Step 2: Run tests**
  Run: `bb test`
  Expected: PASS. If anything fails, fix the SDK, not the test, unless the Python test relies on Python-only behaviour; record such cases in `docs/parity.md`.

- [x] **Step 3: Commit**
  `git commit -m "Add retry and cancellation parity tests"`

> Deviation: added `default-cancellation-applied-at-execution` (the Go `TestDefaultCancellationApplied` flow) alongside the Python ports. No SDK changes were needed.

### Task 8: Worker

**Files:**
- Modify: `src/absurd/core.clj`
- Create: `test/absurd/worker_test.clj`

- [x] **Step 1: Write tests**
  Port: `test_sync_start_worker_uses_threads_for_concurrency` (N slow tasks complete in well under N times their duration with `:concurrency N`), `test_sync_start_worker_keeps_claiming_while_slow_task_runs`, Go `TestRunWorkerShutdownDrainsClaimedTasks` (stop while a task is in flight; task completes; no second run created; `stop-worker!` returns `true`), Go `TestRunWorkerRecoversTaskPanics` (handler throws `Error`; worker keeps processing the next task), and two lease-watchdog tests: (a) `:claim-timeout 1`, handler sleeps 1.5 s, `:on-lease-timeout` hook is called once with the ctx; (b) `:claim-timeout 1`, handler returns immediately, then the test waits 1.5 s and asserts the hook was never called (cancellation on completion). Also: `stop-worker!` with `:shutdown-timeout 0.2` while a 1 s task runs returns `false` and the task still completes afterwards. Worker tests use a `DataSource` built from the container JDBC URL rather than `*conn*`, because the pool threads need their own connections; create the queue through the shared connection first.

- [x] **Step 2: Run tests, expect failure**
  Run: `bb test`
  Expected: FAIL

- [x] **Step 3: Implement worker**
  `start-worker!`, `stop-worker!`, the loop, the pool, the watchdog, `Closeable` handle. Wire `heartbeat!` and `complete-step` to reschedule the watchdog when `::lease` is present.

- [x] **Step 4: Run tests, expect pass**
  Run: `bb test`
  Expected: PASS

- [x] **Step 5: Commit**
  `git commit -m "Add polling worker with thread pool and lease watchdog"`

> Deviation: the worker handle is a `reify` of `Closeable`, `IDeref` and a private `WorkerControl` protocol (used by `stop-worker!`), with the status map holding `:worker-id`, `:queue`, `:concurrency`, `:running?` and, after stopping, `:drained?`.
> Deviation: `locking` is replaced by a `ReentrantLock` helper because cloverage cannot instrument `monitor-enter`; `ProcessHandle` needs an explicit import on Clojure's default import list.
> Deviation: the watchdog is first scheduled right after the context is built (inside `run-handler!`), so the unknown-task and queue-mismatch paths never arm it.

> Codex review (fixed): `stop-worker!` now interrupts the poll sleep and bounds both the loop-thread join and pool termination by `:shutdown-timeout`; `start-worker!` throws `:absurd/worker-already-running` instead of orphaning a running worker. Covered by `only-one-worker-per-client` and `stop-worker-returns-promptly-from-a-long-poll-sleep`.

> Codex review round 2 (fixed): the poll loop now waits on a stop latch instead of being interrupted, claimed tasks are always dispatched (`acquireUninterruptibly`), and the loop-thread join is skipped when the shutdown budget is already exhausted since `join(0)` blocks forever.

### Task 9: Replay determinism property test

**Files:**
- Create: `test/absurd/determinism_test.clj`

- [x] **Step 1: Write the generator and property**
  Generator: recursive `gen/recursive-gen` over `gen/one-of [gen/boolean gen/large-integer (gen/double* {:NaN? false :infinite? false}) gen/string gen/keyword (gen/return nil)]` with containers `gen/vector` and `gen/map` keyed by `gen/keyword` or `gen/string-alphanumeric`. Property (20 trials, each a fresh task on the shared queue): handler records the `step` return into an atom, throws on attempt one; after two `work-batch!` calls, the two recorded values are `=` and the task is completed. Also assert the value equals `(absurd.json/round-trip generated)`.

- [x] **Step 2: Run**
  Run: `bb test`
  Expected: PASS

- [x] **Step 3: Commit**
  `git commit -m "Add replay determinism property test"`

> Deviation: the string generator avoids NUL characters and lone surrogates because Postgres jsonb rejects them; keyword keys are generated without namespaces because the codec writes keys by `name`.

### Task 10: Cross-SDK compatibility check

**Files:**
- Create: `cross-sdk/compose.yaml`, `cross-sdk/python/worker.py`, `cross-sdk/python/client.py`, `cross-sdk/run.sh`, `cross-sdk/clj/cross_sdk/main.clj` (a `-M` main on the `:cross-sdk` alias extra path; nothing is added under `src/`)
- Modify: `bb.edn`, `.github/workflows/checks.yaml`, `deps.edn` (`:cross-sdk` alias with `:extra-paths ["cross-sdk/clj"]`)

- [x] **Step 1: Compose and scripts**
  Two queues: `xsdk_py` (worked by Python) and `xsdk_clj` (worked by Clojure). Two tasks with the same shape: `py-echo` and `clj-echo` each run one step, sleep 1 second, await event `go:<task_id>`, and return `{"from": "<sdk>", "params": params}`. Events are cached first-emit-wins, so each spawner emits `go:<task_id>` immediately after spawning; no one needs to wait for the task to reach the await.

  `compose.yaml`: `postgres:16-alpine` with two init scripts mounted into `/docker-entrypoint-initdb.d/`: `01-absurd.sql` (the vendored schema) and `02-queues.sql` (`select absurd.create_queue('xsdk_py'); select absurd.create_queue('xsdk_clj');`), so both queues exist before any worker or spawner connects; a `python-worker` service (`python:3.12-slim`, `pip install absurd-sdk==0.5.0`, runs `worker.py` on `xsdk_py`), and a `python-client` service (same image, `profiles: [client]`, runs `client.py`).

  `worker.py`: registers `py-echo` and calls `start_worker()`.
  `client.py`: spawns `clj-echo` on `xsdk_clj`, emits `go:<task_id>` on `xsdk_clj`, awaits the result with a 60 s timeout, checks `from == "clojure"`, then emits `xsdk:done` on `xsdk_clj`; exits 1 on timeout or mismatch.

  `cross-sdk/clj/cross_sdk/main.clj`: registers `clj-echo`, starts a worker on `xsdk_clj`, then runs the forward check: spawns `py-echo` on `xsdk_py`, emits `go:<task_id>` on `xsdk_py`, awaits the result (60 s), checks `from = "python"`. It then keeps the worker alive until it sees event `xsdk:done` on `xsdk_clj` (poll `absurd.e_xsdk_clj` through `absurd.sql`, or simpler: spawn a tiny `wait-done` task on `xsdk_clj` that awaits `xsdk:done`, and await its result with a 120 s timeout). Exits 0 when both the forward check passed and `xsdk:done` arrived; exits 1 on any timeout or mismatch.

  `run.sh`: `docker compose up -d postgres python-worker`, wait for Postgres health, run the Clojure main in the background with `DATABASE_URL` pointing at the exposed port, run `docker compose run --rm python-client`, wait for the Clojure main and collect its exit code, `docker compose down -v`, exit non-zero if either side failed.

- [x] **Step 2: bb task and CI job**
  `bb cross-sdk-test` runs `cross-sdk/run.sh`. Add a `cross-sdk` job to `checks.yaml` after `tests`, using the same mise action and Docker (available on `ubuntu-latest`).

- [x] **Step 3: Run locally**
  Run: `bb cross-sdk-test`
  Expected: both directions print success, exit 0.

- [x] **Step 4: Commit**
  `git commit -m "Add cross-SDK compatibility check against the Python SDK"`

> Deviation: Postgres publishes a random host port (`127.0.0.1::5432`) and `run.sh` discovers it with `docker compose port`, so the check never collides with a local Postgres. A named `pip-cache` volume keeps the two `pip install` runs fast.
> Deviation: the Python client emits `xsdk:done` with `{"ok": bool}` even on failure so the Clojure side always terminates; the Clojure main exits non-zero unless both directions succeeded.

### Task 11: Documentation

**Files:**
- Modify: `README.md`, `CHANGELOG.md`
- Create: `docs/parity.md`

- [x] **Step 1: README**
  Sections: install (git dep coordinates since unpublished), schema setup pointer to absurdctl, client creation (DataSource, Connection, URL, env defaults), registering tasks, spawning, context operations with one example each, worker lifecycle with `with-open` and integrant-style start/stop, hooks with the "must propagate exceptions" rule, errors table, JSON round-trip rule, testing notes (Docker required, `bb cross-sdk-test`). Follow /writing-clearly.

- [x] **Step 2: parity.md**
  Table: every Python sync test name, the Clojure test var, or `skipped` with reason (async-only, decorator-only).

- [x] **Step 3: CHANGELOG**
  Under `Unreleased`: list the public API as added, note schema target 0.5.0.

- [x] **Step 4: Full check and commit**
  Run: `bb check`
  Expected: fmt, lint, outdated, tests all pass.
  `git commit -m "Document the SDK API and test parity"`

> Deviation: `bb check` runs the template's `outdated` task, which upgrades dependencies in place. The suite passed on the upgraded set (Clojure 1.12.6, next.jdbc 1.3.1118, postgresql 42.7.14, jsonista 1.0.2, tools.logging 1.3.1, test.check 1.1.3, logback 1.6.5, deps-deploy 0.2.5), so the upgrade is kept.

---

## Completion summary

**Implemented.** The full first cut: `absurd.json` (codec), `absurd.sql` (one
function per stored procedure with AB001/AB002 signal mapping), and
`absurd.core` (client map, registry, spawn and control plane, task context with
steps, sleeps, event waits, durable task-result waits, heartbeats, single-task
execution, `work-batch!`, and a thread-pool worker with a lease watchdog and a
Closeable handle). Test suite: 114 tests / 442 assertions covering every
synchronous Python SDK test (see `docs/parity.md`), the ported Go parity
tests, a test.check replay-determinism property, and worker/lease behaviour.
The cross-SDK check (`bb cross-sdk-test`) runs tasks across this SDK and
`absurd-sdk==0.5.0` in both directions and passes. `bb check` is green.

**Verification.** Full suite via `bb check` after the final commit; end-to-end
exercised through the cross-SDK check twice (before and after the dependency
upgrade), which drives the library the way a user would: client from a URL,
registration, `start-worker!`, `spawn!`, `emit-event!`, `await-task-result`.

**Issues encountered.**
- Testcontainers 1.x cannot talk to the Docker 29 daemon (API 1.32 rejected);
  switched to Testcontainers 2.0.5.
- cloverage cannot instrument `locking` and refuses an empty `src/`; a
  `ReentrantLock` helper replaced `locking`, and Task 1 was verified with
  eftest directly.
- `absurd.await_event` rejects negative timeouts; the SDK clamps them to 0.
- `bb check` upgrades dependencies in place; the upgraded set is kept.

**Deviations (all recorded under their tasks).**
- Task 1: Testcontainers 2.0.5; `test-client` helper deferred to Task 4;
  eftest-only verification; logback test config; `.tmp/` ignored.
- Task 2: Jackson `USE_LONG_FOR_INTS`.
- Task 3: `with-task-state-errors` catches `SQLException`; its helper is public.
- Task 4: execution-dependent tests moved to Task 5; `get-queue-policy`
  keywordizes enums; `->jdbc-url` added (with credential encoding).
- Task 5: private fns reached via `ns-resolve` in two tests; codex notes on
  `name#2` collisions and non-run-scoped fallback reads kept for upstream parity.
- Task 6: negative event timeouts clamped; some task-result tests live in
  `spawn_test.clj`; event-timeout checkpointing kept as upstream; lease pacing
  fixed in context waits.
- Task 7: Go default-cancellation flow added.
- Task 8: `reify` handle with a private protocol; `ReentrantLock` instead of
  `locking`; watchdog armed after context creation; later fixups bounded
  shutdown, one worker per client, latch-based stop, zero-join guard.
- Task 9: generator avoids NUL/surrogates and namespaced keywords.
- Task 10: random host port, pip cache volume, done-signal always emitted.
- Task 11: dependency upgrade from `bb check`.

**What the plan could have specified better.** It should have said that
`bb check` upgrades dependencies, that cloverage needs a non-empty `src/` for
Task 1's verification, and that Task 4's Python test list includes tests that
require `work-batch!` (which only exists after Task 5). It also assumed the
pinned Testcontainers version would work with the local Docker daemon.
