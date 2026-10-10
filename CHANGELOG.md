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
- `absurd.json`: jsonista codec with keyword keys and jsonb helpers.
- Test suite ported from the Python SDK (see `docs/parity.md`), a replay
  determinism property test, and a cross-SDK check against the Python SDK
  (`bb cross-sdk-test`).
