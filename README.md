# absurd-clj

[![CI](https://github.com/abogoyavlensky/absurd-clj/actions/workflows/checks.yaml/badge.svg?branch=main)](https://github.com/abogoyavlensky/absurd-clj/actions/workflows/checks.yaml)

Clojure SDK for [Absurd](https://github.com/earendil-works/absurd), a durable
execution workflow system built entirely on Postgres.

Targets Absurd schema version `0.5.0`. Not published yet.

## Status

Work in progress. The API is being shaped after the official
[Python](https://earendil-works.github.io/absurd/sdks/python/) and
[Go](https://earendil-works.github.io/absurd/sdks/go/) SDKs.

## Development

### Requirements

Install Java, Clojure and Babashka manually or via [mise](https://mise.jdx.dev/):

```shell
mise install
```

Docker is required to run the test suite.

*Note: Check versions in `.mise.toml` file.*

### Manage project

All management tasks:

```shell
bb tasks
```

Run all checks and tests:

```shell
bb check
```

Install the library into the local Maven repository:

```shell
bb install
```

## License

MIT License
Copyright (c) 2026 Andrey Bogoyavlenskiy
