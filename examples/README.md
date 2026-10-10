# Examples

Two ways to see absurd-clj working, both against the same local stack.

| Example | What it shows |
|---------|---------------|
| [quickstart](quickstart/) | Two scripts, a worker and a client, mirroring the upstream Python and Go quickstarts. Start here. |
| [app](app/) | A small application wired with Integrant: connection pool, Absurd client, worker lifecycle, logging, and a CLI to drive it. |

## Start the stack

Requires Docker.

```shell
cd examples
docker compose up -d --build
```

This starts:

- Postgres 16 on `localhost:5433` (user `absurd`, password `absurd`, database `absurd`) with the Absurd 0.5.0 schema applied and a `default` queue created.
- [Habitat](https://earendil-works.github.io/absurd/tools/habitat/), the Absurd dashboard, on <http://localhost:7890>.

Set `ABSURD_PG_PORT` or `HABITAT_PORT` before `docker compose up` if those ports are taken.

Stop and remove everything, including the database volume:

```shell
docker compose down -v
```

## Verify with Habitat

Open <http://localhost:7890> while an example runs. The task list shows each
task's state (`pending`, `running`, `sleeping`, `completed`, `failed`); a task
page lists its runs, checkpoints and events. The `provision-user` task in both
examples fails once on purpose, so you can see the retry as a second run and
the checkpoints that survived it.
