# Test parity with the Python SDK

Every synchronous test in the upstream Python SDK (`sdks/python/tests` at
Absurd 0.5.0) has a port here, plus selected Go SDK parity tests. Async-only
tests have no equivalent because this SDK has no async client, and the
`run_step` decorator tests are skipped because the decorator form is not part
of the Clojure API.

## test_absurd.py

| Python | Clojure |
|--------|---------|
| test_sync_worker_processes_task | absurd.execution-test/sync-worker-processes-task |
| test_sync_worker_suspends_until_alarm | absurd.sleep-test/sync-worker-suspends-until-alarm |
| test_sync_start_worker_uses_threads_for_concurrency | absurd.worker-test/start-worker-uses-threads-for-concurrency |
| test_sync_start_worker_keeps_claiming_while_slow_task_runs | absurd.worker-test/start-worker-keeps-claiming-while-slow-task-runs |
| test_unknown_task_is_deferred_not_failed | absurd.execution-test/unknown-task-is-deferred-not-failed |
| test_unknown_task_defer_failure_preserves_error_context | absurd.execution-test/unknown-task-defer-failure-preserves-error-context |
| test_async_* (4) | skipped: async only |

## test_connection_defaults.py

| Python | Clojure |
|--------|---------|
| test_sync_absurd_prefers_absurd_database_url_over_pgdatabase | absurd.client-test/resolve-database-url-follows-upstream-order |
| test_sync_absurd_falls_back_to_pgdatabase | absurd.client-test/resolve-database-url-follows-upstream-order |
| test_async_absurd_falls_back_to_default_absurd_uri | absurd.client-test/resolve-database-url-follows-upstream-order (as sync) |

## test_events.py

| Python | Clojure |
|--------|---------|
| test_await_and_emit_event_flow | absurd.events-test/await-and-emit-event-flow |
| test_event_emitted_before_await_is_cached | absurd.events-test/event-emitted-before-await-is-cached |
| test_emit_event_is_first_write_wins | absurd.events-test/emit-event-is-first-write-wins |
| test_await_event_with_timeout_expires_and_wakes_task | absurd.events-test/await-event-with-timeout-expires-and-wakes-task |
| test_multiple_tasks_can_await_same_event | absurd.events-test/multiple-tasks-can-await-same-event |
| test_await_event_timeout_does_not_recreate_wait_on_resume | absurd.events-test/await-event-timeout-does-not-recreate-wait-on-resume |

## test_heartbeat.py

| Python | Clojure |
|--------|---------|
| test_sync_heartbeat_zero_is_rejected_by_sql | absurd.heartbeat-test/heartbeat-zero-is-rejected-by-sql |
| test_sync_heartbeat_on_failed_run_raises_failed_task | absurd.heartbeat-test/heartbeat-on-failed-run-raises-failed-run |
| test_sync_execute_task_swallows_failed_task_checkpoint | absurd.heartbeat-test/execute-task-swallows-failed-run-on-checkpoint |
| test_sync_execute_task_swallows_cancelled_task_on_complete_run | absurd.heartbeat-test/execute-task-swallows-cancelled-on-complete-run |
| test_sync_execute_task_swallows_cancelled_task_on_fail_run | absurd.heartbeat-test/execute-task-swallows-cancelled-on-fail-run |
| test_async_* (4) | skipped: async only |

## test_hooks.py

| Python | Clojure |
|--------|---------|
| test_sync_before_spawn_can_inject_headers | absurd.execution-test/before-spawn-headers-are-visible-in-the-context |
| test_sync_before_spawn_preserves_existing_headers | absurd.spawn-test/before-spawn-hook-can-inject-and-preserve-headers |
| test_sync_wrap_task_execution | absurd.execution-test/wrap-task-execution-hook-wraps-the-handler |
| test_sync_contextvar_propagation | absurd.execution-test/hooks-propagate-dynamic-context-through-headers |
| get_current_context tests | absurd.execution-test/current-context-is-bound-inside-handlers |
| test_async_* | skipped: async only |

## test_idempotency.py

| Python | Clojure |
|--------|---------|
| test_spawn_with_idempotency_key_creates_task | absurd.spawn-test/spawn-with-idempotency-key-creates-task |
| test_spawn_with_same_idempotency_key_returns_existing_task | absurd.spawn-test/spawn-with-same-idempotency-key-returns-existing-task |
| test_spawn_without_idempotency_key_always_creates_new_task | absurd.spawn-test/spawn-without-idempotency-key-always-creates-new-task |
| test_different_idempotency_keys_create_separate_tasks | absurd.spawn-test/different-idempotency-keys-create-separate-tasks |
| test_idempotency_key_persists_after_task_completes | absurd.execution-test/idempotency-key-persists-after-task-completes |
| test_idempotent_spawn_executes_only_once | absurd.execution-test/idempotent-spawn-executes-only-once |
| test_async_spawn_with_idempotency_key | skipped: async only |

## test_queue_policy.py and test_queue_validation.py

| Python | Clojure |
|--------|---------|
| test_sync_queue_policy_methods | absurd.client-test/queue-policy-methods |
| test_sync_constructor_fails_before_connect_on_empty_queue | absurd.client-test/constructor-fails-before-connect-on-empty-queue |
| test_sync_queue_validation_is_permissive_but_bounded | absurd.client-test/queue-validation-is-permissive-but-bounded |
| test_async_* (2) | skipped: async only |

## test_retry.py and test_retry_task.py

| Python | Clojure |
|--------|---------|
| test_fail_run_without_strategy_requeues_immediately | absurd.retry-test/fail-run-without-strategy-requeues-immediately |
| test_exponential_backoff_retry_strategy | absurd.retry-test/exponential-backoff-retry-strategy |
| test_fixed_backoff_retry_strategy | absurd.retry-test/fixed-backoff-retry-strategy |
| test_task_fails_permanently_after_max_attempts | absurd.retry-test/task-fails-permanently-after-max-attempts |
| test_registered_task_without_retry_default_uses_client_default | absurd.retry-test/registered-task-without-retry-default-uses-client-default |
| test_cancellation_by_max_duration | absurd.retry-test/cancellation-by-max-duration |
| test_cancellation_by_max_delay | absurd.retry-test/cancellation-by-max-delay |
| test_manual_cancel_pending_task | absurd.retry-test/manual-cancel-pending-task |
| test_manual_cancel_running_task | absurd.retry-test/manual-cancel-running-task |
| test_cancel_blocks_checkpoint_writes | absurd.retry-test/cancel-blocks-checkpoint-writes |
| test_cancel_is_idempotent | absurd.retry-test/cancel-is-idempotent |
| test_cancelling_completed_task_is_noop | absurd.retry-test/cancelling-completed-task-is-noop |
| test_cancelling_failed_task_is_noop | absurd.retry-test/cancelling-failed-task-is-noop |
| test_cancel_non_existent_task_errors | absurd.retry-test/cancel-non-existent-task-errors |
| test_retry_task_defaults_to_one_more_attempt | absurd.execution-test/retry-task-defaults-to-one-more-attempt |
| test_retry_task_spawn_new_creates_fresh_task | absurd.execution-test/retry-task-spawn-new-creates-fresh-task |
| test_async_retry_task | skipped: async only |

## test_step.py and test_task_context.py

| Python | Clojure |
|--------|---------|
| test_step_executes_and_returns_value | absurd.step-test/step-executes-and-returns-value |
| test_step_result_is_cached_and_not_reexecuted_on_retry | absurd.step-test/step-result-is-cached-and-not-reexecuted-on-retry |
| test_begin_complete_step_result_is_cached_and_not_reexecuted_on_retry | absurd.step-test/begin-complete-step-result-is-cached-and-not-reexecuted-on-retry |
| test_multistep_task_only_reexecutes_uncompleted_steps | absurd.step-test/multistep-task-only-reexecutes-uncompleted-steps |
| test_repeated_step_names_work_correctly | absurd.step-test/repeated-step-names-work-correctly |
| test_repeated_begin_complete_step_names_auto_number_correctly | absurd.step-test/repeated-begin-complete-step-names-auto-number-correctly |
| test_failed_step_does_not_save_checkpoint_and_reexecutes | absurd.step-test/failed-step-does-not-save-checkpoint-and-reexecutes |
| test_sleep_for_suspends_until_duration_elapses | absurd.sleep-test/sleep-for-suspends-until-duration-elapses |
| test_sleep_for_schedules_relative_to_database_clock | absurd.sleep-test/sleep-for-schedules-relative-to-database-clock |
| test_sleep_until_checkpoint_prevents_rescheduling | absurd.sleep-test/sleep-until-checkpoint-prevents-rescheduling |
| test_run_step_decorator_* (4) | skipped: decorator form not provided |
| test_step_persists_and_reuses_checkpoints | absurd.step-test/step-persists-and-reuses-checkpoints |
| test_sleep_for_schedules_future_run | absurd.sleep-test/sleep-for-schedules-future-run-from-a-context |
| test_await_event_flow | absurd.events-test/await-event-flow-from-a-context |

## test_task_result.py

| Python | Clojure |
|--------|---------|
| test_fetch_task_result_returns_none_for_unknown_task | absurd.spawn-test/fetch-and-await-task-result-without-a-worker |
| test_fetch_task_result_tracks_lifecycle | absurd.task-result-test/fetch-task-result-tracks-lifecycle |
| test_await_task_result_times_out | absurd.spawn-test/fetch-and-await-task-result-without-a-worker |
| test_task_context_await_task_result_waits_for_terminal_state | absurd.task-result-test/task-context-await-task-result-waits-for-terminal-state |
| test_task_context_await_task_result_rejects_same_queue | absurd.task-result-test/task-context-await-task-result-rejects-same-queue |
| test_async_* (2) | skipped: async only |

## Go SDK parity tests ported

| Go | Clojure |
|----|---------|
| TestSpawnOptionsParity | absurd.spawn-test/spawn-options-parity |
| TestUnknownTaskRequiresExplicitQueueAtSpawn | absurd.spawn-test/unknown-task-requires-explicit-queue-at-spawn |
| TestDefaultCancellationApplied | absurd.spawn-test/default-cancellation-from-registration-is-applied, absurd.retry-test/default-cancellation-applied-at-execution |
| TestQueueMismatchFailsClaimedRunImmediately | absurd.execution-test/queue-mismatch-fails-claimed-run-immediately |
| TestFailureIncludesTraceback | absurd.execution-test/failure-includes-traceback |
| TestInvalidHeadersFailRun | absurd.execution-test/invalid-headers-fail-run |
| TestTaskContextAwaitTaskResultCheckpointSurvivesChildCleanupOnRetry | absurd.task-result-test/task-context-await-task-result-checkpoint-survives-child-cleanup-on-retry |
| TestTaskContextAwaitTaskResultHeartbeatUsesReceiverState | absurd.task-result-test/task-context-await-task-result-heartbeats-while-waiting |
| TestAwaitTaskResultNegativeTimeoutTimesOutImmediately | absurd.spawn-test/fetch-and-await-task-result-without-a-worker |
| TestAwaitEventNegativeTimeoutDoesNotSleepForever | absurd.events-test/await-event-negative-timeout-does-not-sleep-forever |
| TestRunWorkerRecoversTaskPanics | absurd.worker-test/worker-recovers-from-handler-errors |
| TestRunWorkerShutdownDrainsClaimedTasks | absurd.worker-test/worker-shutdown-drains-claimed-tasks |

## Clojure-only tests

- `absurd.determinism-test/step-values-are-identical-across-replay`: test.check property over generated JSON-shaped values.
- `absurd.step-test/step-values-round-trip-through-json-on-first-run`
- `absurd.task-result-test/task-context-await-task-result-keeps-lease-alive-under-competing-claims`
- `absurd.worker-test/lease-watchdog-warns-and-calls-the-hook`, `heartbeat-reschedules-the-watchdog`, `stop-worker-returns-false-when-shutdown-times-out`, `only-one-worker-per-client`, `stop-worker-returns-promptly-from-a-long-poll-sleep`, `worker-handle-is-closeable-and-registered-on-the-client`
- `absurd.client-test/jdbc-url-conversion`, `client-is-an-open-map`, `client-from-url-opens-its-own-datasource`
- `absurd.spawn-test/header-keys-are-stored-as-written`, `max-attempts-precedence`, `registered-task-rejects-mismatched-queue`, `emit-event-validates-name`
- `absurd.sql-test/*` and `absurd.json-test/*`: unit coverage of the lower layers.
- The cross-SDK check (`bb cross-sdk-test`) runs tasks across this SDK and the Python SDK in both directions.
