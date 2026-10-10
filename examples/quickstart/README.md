# Quickstart

A worker and a client, the same flow as the upstream Python and Go
quickstarts. The `provision-user` task:

1. creates a user record (a `step`),
2. fails once on purpose after that checkpoint (`begin-step` / `complete-step`),
3. sends an activation email on the retry (another `step`),
4. waits for a `user-activated:<user-id>` event (`await-event`),
5. returns the final result.

Files:

- `src/quickstart/worker.clj`: registers the task and runs a worker.
- `src/quickstart/client.clj`: spawns a task and inspects or awaits its result.
- `src/quickstart/emit.clj`: emits the event the task waits for.

## Run

Start the stack from the parent directory first (see [examples/README.md](../README.md)):

```shell
cd examples && docker compose up -d --build
```

Then, from this directory, in three terminals:

```shell
# 1. the worker
clojure -M:worker

# 2. spawn a task and wait for it
clojure -M:client --await alice alice@example.com

# 3. let it finish
clojure -M:emit alice
```

The worker prints the step it is running, then "waiting for
user-activated:alice". After the emit, the client prints the final snapshot:

```clojure
final snapshot: {:state :completed, :result {:user-id "alice", :email "alice@example.com", ...}}
```

Without `--await` the client only spawns and prints the current snapshot.

Events are cached per queue and the first emit wins, so re-running the flow
for the same user id finds `user-activated:alice` already emitted and the task
completes without waiting. Use a new user id each time, or reset the stack
with `docker compose down -v`.

Set `ABSURD_DATABASE_URL` to point at another database.

## What to look at in Habitat

Open <http://localhost:7890>:

- The task has two runs: the first failed with "temporary email provider outage", the second completed.
- The `create-user-record` checkpoint was written by the first run and reused by the second; the worker log shows "creating user record" only once.
- While the task waits, its state is `sleeping` and the run shows `wake_event = user-activated:alice`.

Press Ctrl-C in the worker terminal to stop it; in-flight tasks finish first.
