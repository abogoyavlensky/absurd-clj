# Integrant app

A small application that shows where absurd-clj fits in a real service: a
connection pool, the Absurd client with registered tasks and logging hooks,
and a worker whose lifecycle is managed by [Integrant](https://github.com/weavejester/integrant).
Configuration is read with [aero](https://github.com/juxt/aero), so every
value can come from the environment.

Files:

- `resources/config.edn`: the system. Three components: `:app/db` (HikariCP pool), `:app/absurd` (client), `:app/worker`.
- `src/app/system.clj`: Integrant `init-key` / `halt-key!` for each component.
- `src/app/tasks.clj`: task handlers and their registration.
- `src/app/main.clj`: a CLI that starts only the parts a command needs.
- `resources/logback.xml`: logging; the SDK logs through `clojure.tools.logging`.

The worker is started in `init-key :app/worker` and stopped in `halt-key!`
with `stop-worker!`, which waits for in-flight tasks. Spawning and emitting
commands start just the pool and the client.

## Run

Start the stack from the parent directory first (see [examples/README.md](../README.md)):

```shell
cd examples && docker compose up -d --build
```

Then, from this directory:

```shell
# terminal 1: the worker (Ctrl-C stops it after in-flight tasks finish)
clojure -M:run worker

# terminal 2
clojure -M:run hello Ada                      # spawn, wait, print the result
clojure -M:run spawn alice alice@example.com  # starts the provision-user flow
clojure -M:run activate alice                 # lets it finish
clojure -M:run result <task-id>               # snapshot of any task
```

Expected worker log for `spawn alice`: attempt 1 creates the user record and
fails with the simulated outage; attempt 2 skips the record step, sends the
email and waits; after `activate alice` the task completes.

Environment variables: `APP_JDBC_URL`, `APP_QUEUE`, `APP_WORKER_ID`.

Events are cached per queue and the first emit wins, so re-running the flow
for the same user id finds `user-activated:alice` already emitted and the task
completes without waiting. Use a new user id each time, or reset the stack
with `docker compose down -v`.

## What to look at in Habitat

Open <http://localhost:7890>:

- `hello` tasks complete in a single run with one checkpoint-free result.
- `provision-user` tasks show two runs. The first run's failure reason carries the exception name, message and stack trace; the second run reuses the `create-user-record` and `demo-transient-outage` checkpoints.
- Stop the worker with Ctrl-C while a task is sleeping: the task stays `sleeping` and resumes when a worker starts again.
