# absurd-clj

[![CI](https://github.com/abogoyavlensky/absurd-clj/actions/workflows/checks.yaml/badge.svg?branch=main)](https://github.com/abogoyavlensky/absurd-clj/actions/workflows/checks.yaml)

Clojure SDK for [Absurd](https://github.com/earendil-works/absurd), a durable
execution workflow system built entirely on Postgres. It can also run on
SQLite, for single-process apps that want durable execution with one database
file and no database server (see [SQLite backend](#sqlite-backend)).

Absurd keeps all the durable-execution logic in Postgres stored procedures.
This library is the thin Clojure layer on top: it registers task handlers,
spawns tasks, runs workers, and checkpoints steps, sleeps and event waits so a
task survives crashes and retries without repeating finished work.

It targets Absurd schema version `0.5.0` and follows the official
[Python](https://earendil-works.github.io/absurd/sdks/python/) and
[Go](https://earendil-works.github.io/absurd/sdks/go/) SDKs closely enough that
workers written in different languages can share queues.

## Status

Alpha. Not published to Clojars yet. Use it as a git dependency:

```clojure
{:deps {io.github.abogoyavlensky/absurd-clj {:git/sha "<commit>"}}}
```

You also need a Postgres JDBC driver on the classpath. The library depends on
`org.postgresql/postgresql`, so nothing extra is required unless you want a
different version.

## Setup

Apply the Absurd schema and create a queue with
[absurdctl](https://earendil-works.github.io/absurd/tools/absurdctl/):

```bash
uvx absurdctl init -d mydb
uvx absurdctl create-queue -d mydb default
```

Or create queues from Clojure with `create-queue!` once the schema exists.

## Creating a client

```clojure
(require '[absurd.core :as absurd])

;; From a connection string
(def app (absurd/client {:db "postgresql://user:pass@localhost:5432/mydb"
                         :queue "default"}))

;; From an existing DataSource or Connection (next.jdbc compatible)
(def app (absurd/client {:db datasource :queue "default"}))

;; Minimal: uses ABSURD_DATABASE_URL, then PGDATABASE,
;; then postgresql://localhost/absurd; queue defaults to "default"
(def app (absurd/client))
```

A client is a plain map. Options:

| Key             | Default                | Meaning                                                   |
|-----------------|------------------------|-----------------------------------------------------------|
| `:db`           | env-resolved URL       | `DataSource`, `java.sql.Connection`, URL string, or nil (nil needs Postgres) |
| `:backend`      | inferred               | `:postgres` or `:sqlite`; a `jdbc:sqlite:` URL infers `:sqlite`, anything else `:postgres` |
| `:queue`        | `"default"`            | Default queue for spawning and workers                    |
| `:max-attempts` | `5`                    | Default retry limit                                       |
| `:hooks`        | `{}`                   | `:before-spawn`, `:wrap-task-execution`, `:on-lease-timeout` |
| `:now-fn`       | `Instant/now`          | Clock used to compute sleep wake times                    |
| `:db-now-fn`    | `Instant/now`          | SQLite only: the engine clock, standing in for Postgres `absurd.current_time()` |

URLs in `postgresql://` or `postgres://` form are converted to JDBC URLs;
credentials in the URL become `user` and `password` parameters. A `jdbc:`
URL is used as is.

`(absurd/close! app)` stops a running worker.

## SQLite backend

The same API runs on SQLite. Absurd's engine lives in Postgres stored
procedures; for SQLite this library ports them to Clojure (`absurd.sqlite.engine`)
behind the `absurd.store/Store` protocol, and the Postgres test suite runs
against both backends. The idea follows Simon Willison's
[Absurd in SQLite](https://simonwillison.net/2025/Nov/12/absurd-in-sqlite/)
proof of concept.

Add the driver yourself, the same way you add a connection pool. The library
has no compile-time dependency on it, so Postgres users never download it:

```clojure
{:deps {org.xerial/sqlite-jdbc {:mvn/version "3.53.4.0"}}}
```

```clojure
;; From a URL: a "jdbc:sqlite:" URL infers the backend
(def app (absurd/client {:db "jdbc:sqlite:app.db" :queue "default"}))

;; From a DataSource or Connection: say which backend it is
(def app (absurd/client {:db datasource :backend :sqlite}))
```

The app creates the schema (`absurd_*` tables, so they can live in your
application's own database file) explicitly, before the first operation. The
library never creates it on its own, and the client constructor never connects.
Operations on a database without the schema throw `:absurd/schema-missing`.
Pick one of two ways:

1. Call `ensure-schema!` at boot. It creates the tables when missing, switches
   the file to WAL journaling, checks the schema version, and is safe to call
   on every start:

   ```clojure
   (require '[absurd.sqlite :as sqlite])
   (sqlite/ensure-schema! datasource)
   ```

2. Apply the migration with your own migration tool. The library ships
   `absurd/sqlite/migrations/001-absurd-init.up.sql` and `.down.sql` on the
   classpath. With ragtime, load them next to your own migrations (the
   migration ID is `001-absurd-init`):

   ```clojure
   (ragtime.repl/migrate
     {:datastore (ragtime.next-jdbc/sql-database ds)
      :migrations (concat (ragtime.next-jdbc/load-resources "absurd/sqlite/migrations")
                          (ragtime.next-jdbc/load-resources "migrations"))})
   ```

   With migratus or Flyway, copy the file into your migrations directory under
   your own name (migratus identifies migrations by their numeric prefix, so
   `001` would clash with your first migration). A migration runs in a
   transaction and cannot switch the journal mode, so set WAL on the
   connection: `jdbc:sqlite:app.db?journal_mode=WAL`. WAL is not required for
   correctness, but it lets reads run alongside a write.

Future schema versions ship as new numbered files; `001` never changes.

Each operation is one SQLite transaction (`BEGIN IMMEDIATE` for writes). Every
operation over the same `DataSource` or `Connection` object takes one
in-process lock, so workers, request threads and several clients (say, one per
queue) can share a single `Connection` safely. Run **one process writing to the
database file**, with any number of threads. Several processes work too but
contend for SQLite's write lock (waiting up to the driver's `busy_timeout`,
3 s by default); that setup is not tested.

Where it differs from Postgres:

- **No partitioned queues.** `create-queue!` with `:storage-mode :partitioned`
  throws.
- **Policy intervals are stored verbatim.** `set-queue-policy!` accepts
  `<n> second|minute|hour|day|week[s]` and keeps the text as given, where
  Postgres normalizes `"12345 seconds"` to `"03:25:45"`. Anything else,
  including negative values, throws.
- **No cleanup yet.** `cleanup_ttl` and `cleanup_limit` are stored but never
  enforced, so finished tasks stay in the file. See
  [docs/backlog/sqlite-cleanup.md](docs/backlog/sqlite-cleanup.md).
- **Not shared with other-language SDKs.** The Python and Go SDKs only speak to
  the Postgres procedures, so workers in other languages cannot use a SQLite
  queue.
- **The engine clock is the JVM clock** (`:db-now-fn`, default `Instant/now`),
  not the database's. All timestamps come from the one process that writes.
- **No in-memory URLs.** Every new connection to `jdbc:sqlite::memory:` is a
  fresh empty database, so such a URL is rejected. Open a `Connection` and pass it
  as `:db` with `:backend :sqlite` instead.

## Registering tasks

```clojure
(absurd/register-task! app "send-email"
  (fn [params ctx]
    (let [rendered (absurd/step ctx "render" #(str "<h1>" (:template params) "</h1>"))]
      (absurd/step ctx "send" #(hash-map :accepted [(:to params)] :html rendered))))
  {:max-attempts 10
   :cancellation {:max-duration 3600}})
```

Handlers receive the decoded params (keyword keys) and the task context.
Registration options: `:queue`, `:max-attempts`, `:cancellation`.
`register-task!` returns the client, so registrations thread.

## Spawning tasks

```clojure
(absurd/spawn! app "send-email" {:to "user@example.com" :template "welcome"}
  {:max-attempts 10
   :retry-strategy {:kind :exponential :base-seconds 2 :factor 2 :max-seconds 300}
   :headers {:trace-id "..."}
   :cancellation {:max-duration 3600}
   :idempotency-key "welcome:user-42"})
;; => {:task-id "..." :run-id "..." :attempt 1 :created true}
```

Spawning a task that is not registered in this process requires `:queue`.
Spawning a registered task with a different `:queue` is an error.
Enum values accept keywords or strings (`:exponential` or `"exponential"`).
Header keys are stored exactly as written.

## Task context

Every context operation takes the context first.

| Operation                                               | Purpose                                                        |
|---------------------------------------------------------|----------------------------------------------------------------|
| `(step ctx name f)`                                     | Run `f` once and checkpoint its result                         |
| `(begin-step ctx name)` / `(complete-step ctx handle v)` | Split a step across two calls; handle has `:done?` and `:state` |
| `(sleep-for ctx name seconds)`                          | Suspend for a duration                                         |
| `(sleep-until ctx name instant)`                        | Suspend until an `Instant`, `Date`, epoch seconds or ISO string |
| `(await-event ctx event-name {:timeout :step-name})`    | Suspend until an event is emitted; returns its payload         |
| `(await-task-result ctx task-id {:queue :timeout})`     | Durably wait for another task on a different queue             |
| `(heartbeat! ctx)` / `(heartbeat! ctx seconds)`         | Extend the current run's lease                                 |
| `(emit-event! ctx event-name payload)`                  | Emit an event on the task's queue                              |
| `(current-context)`                                     | The context of the running handler, or nil                     |

The context map exposes `:task-id`, `:run-id`, `:task-name`, `:attempt`,
`:queue` and `:headers`.

Repeated step names are numbered automatically: `name`, `name#2`, `name#3`.

### Step values round-trip through JSON

A step's return value is encoded and decoded before it is returned, so the
first run sees exactly what a replay will see: keyword keys stay keywords,
keyword values become strings, sets become vectors, and integers come back as
`Long`. Convert dates and other non-JSON values yourself.

### Checkpoint compatibility

Checkpoint names and payloads match the Python and TypeScript SDKs, including
the `name#2` numbering, `$awaitEvent:<name>` and `$awaitTaskResult:<task-id>`
defaults, and the sleep checkpoint storing an ISO-8601 wake time. Workers in
different languages can process the same queue. Two upstream behaviours are
kept deliberately: an explicit step named `x#2` collides with the generated
name of a repeated `x`, and an event timeout is not checkpointed, so a handler
that catches it and suspends elsewhere re-registers the wait on replay.

## Task results

```clojure
(absurd/fetch-task-result app task-id)
;; => nil | {:state :pending} | {:state :completed :result v} | {:state :failed :failure v}

(absurd/await-task-result app task-id {:timeout 30})
```

`await-task-result` polls with backoff and throws `:absurd/timeout` when the
timeout expires, or `:absurd/task-not-found` when the task does not exist.

## Events, cancellation, retries

```clojure
(absurd/emit-event! app "order.shipped" {:tracking "XYZ"})
(absurd/emit-event! app "order.shipped" {:tracking "XYZ"} {:queue "orders"})
(absurd/cancel-task! app task-id)
(absurd/retry-task! app task-id {:max-attempts 5 :spawn-new? false})
;; => {:task-id "..." :run-id "..." :attempt 2 :created false}
```

Events are cached: the first emit per name wins, and a task that awaits an
already emitted event resumes immediately.

## Queue management

```clojure
(absurd/create-queue! app "emails")
(absurd/create-queue! app "emails-part" {:storage-mode :partitioned
                                         :cleanup-ttl "90 days"
                                         :cleanup-limit 2000
                                         :detach-mode :empty
                                         :detach-min-age "30 days"})
(absurd/set-queue-policy! app "emails-part" {:cleanup-ttl "60 days"})
(absurd/get-queue-policy app "emails-part")
(absurd/drop-queue! app "emails")
(absurd/list-queues app)
```

## Running workers

```clojure
;; Background worker; the handle is Closeable and deref-able
(def worker (absurd/start-worker! app {:worker-id "web-1"
                                       :claim-timeout 120
                                       :concurrency 4
                                       :poll-interval 250}))
@worker ;; => {:worker-id "web-1" :queue "default" :concurrency 4 :running? true}
(absurd/stop-worker! worker) ;; => true when every task finished before :shutdown-timeout

;; Scoped
(with-open [worker (absurd/start-worker! app {:concurrency 4})]
  ...)

;; One claim-and-execute pass on the calling thread (cron jobs, tests)
(absurd/work-batch! app {:worker-id "cron-1" :claim-timeout 120 :batch-size 10})
```

Worker options: `:worker-id` (hostname:pid), `:claim-timeout` seconds (120),
`:concurrency` (1), `:batch-size` (concurrency), `:poll-interval` ms (250),
`:shutdown-timeout` seconds (60). A client runs at most one worker at a time.
Handlers run on a fixed thread pool and should be ordinary blocking functions.

When a task runs past its claim timeout the worker logs a warning and calls
the `:on-lease-timeout` hook with `(ctx seconds)` if one is configured. The
process is never terminated by the library.

With a component library:

```clojure
;; integrant
(defmethod ig/init-key ::worker [_ {:keys [app]}] (absurd/start-worker! app {:concurrency 4}))
(defmethod ig/halt-key! ::worker [_ worker] (absurd/stop-worker! worker))
```

## Hooks

```clojure
(absurd/client
  {:hooks {:before-spawn (fn [task-name params opts]
                           (update opts :headers assoc :trace-id (current-trace-id)))
           :wrap-task-execution (fn [ctx execute]
                                  (binding [*trace* (get-in ctx [:headers :trace-id])]
                                    (execute)))
           :on-lease-timeout (fn [ctx seconds]
                               (log/warn "lease exceeded" (:task-id ctx)))}})
```

`:wrap-task-execution` must return the result of `(execute)` and must let
exceptions propagate. The SDK uses exceptions to suspend a task; a hook that
swallows them would complete the run with nil.

## Errors

All SDK errors are `ex-info` with a `:type` key:

| `:type`                         | Meaning                                              |
|---------------------------------|------------------------------------------------------|
| `:absurd/timeout`               | `await-event` or `await-task-result` timed out       |
| `:absurd/task-not-found`        | Task result lookup found nothing                     |
| `:absurd/invalid-queue-name`    | Empty or longer than 57 bytes                        |
| `:absurd/invalid-db`            | Unsupported `:db` value, or SQLite without `:db` or with an in-memory URL |
| `:absurd/invalid-backend`       | `:backend` is neither `:postgres` nor `:sqlite`      |
| `:absurd/schema-missing`        | SQLite operation on a database without the schema    |
| `:absurd/schema-version-mismatch` | A SQLite file written by another schema version    |
| `:absurd/queue-mismatch`        | Spawn asked for a queue other than the registered one |
| `:absurd/unregistered-task`     | Spawn of an unknown task without `:queue`            |
| `:absurd/same-queue-wait`       | Context wait on a task in the same queue             |
| `:absurd/worker-already-running`| Second `start-worker!` on one client                 |
| `:absurd/suspend`, `:absurd/cancelled`, `:absurd/failed-run` | Internal control flow; never reach user code outside a hook |

Database errors from the driver propagate unchanged.

## Try it

`examples/` has a quickstart (a worker and a client, two scripts) and a small
application wired with Integrant, both running against a compose stack with
Postgres and the Habitat dashboard. See [examples/README.md](examples/README.md).

## Development

Install Java, Clojure and Babashka manually or via [mise](https://mise.jdx.dev/):

```shell
mise install
```

Docker is required for the tests: they start Postgres through Testcontainers
and load the vendored schema from `test/resources/absurd.sql`. Every test runs
once per backend listed in `ABSURD_TEST_BACKENDS` (comma-separated, default
`postgres,sqlite`); `ABSURD_TEST_BACKENDS=sqlite bb test` runs the suite on
SQLite alone, without Docker.

```shell
bb tasks            # list all tasks
bb check            # format, lint, outdated deps, tests
bb test             # tests with coverage
bb sync-schema      # refresh the vendored schema from the upstream 0.5.0 tag
bb cross-sdk-test   # spawn and work tasks across this SDK and the Python SDK
bb install          # install into the local Maven repository
```

`docs/parity.md` maps every upstream Python SDK test to its port here.

## License

MIT License
Copyright (c) 2026 Andrey Bogoyavlenskiy
