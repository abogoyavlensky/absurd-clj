(ns absurd.store
  "The database boundary of the SDK.

  `Store` has one method per Absurd stored procedure, with the argument order
  and return shapes of `absurd.sql` but taking a store instead of a
  connectable. UUIDs are strings, JSON columns are decoded, `:updated-at` is
  an `Instant` and policy intervals are strings. AB001 and AB002 surface as
  `:absurd/cancelled` and `:absurd/failed-run` ex-info signals; any other
  engine error is a `java.sql.SQLException`.

  `PostgresStore` delegates to the stored procedures through `absurd.sql`."
  (:require [absurd.sql :as sql]))

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

(defrecord PostgresStore [db]
  Store
  (create-queue! [_ queue storage-mode] (sql/create-queue! db queue storage-mode))
  (drop-queue! [_ queue] (sql/drop-queue! db queue))
  (list-queues [_] (sql/list-queues db))
  (get-queue-policy [_ queue] (sql/get-queue-policy db queue))
  (set-queue-policy! [_ queue policy] (sql/set-queue-policy! db queue policy))
  (spawn-task! [_ queue task-name params options]
    (sql/spawn-task! db queue task-name params options))
  (retry-task! [_ queue task-id options] (sql/retry-task! db queue task-id options))
  (cancel-task! [_ queue task-id] (sql/cancel-task! db queue task-id))
  (get-task-result [_ queue task-id] (sql/get-task-result db queue task-id))
  (claim-tasks! [_ queue worker-id claim-timeout qty]
    (sql/claim-tasks! db queue worker-id claim-timeout qty))
  (complete-run! [_ queue run-id result] (sql/complete-run! db queue run-id result))
  (fail-run! [_ queue run-id failure] (sql/fail-run! db queue run-id failure))
  (schedule-run-after! [_ queue run-id seconds]
    (sql/schedule-run-after! db queue run-id seconds))
  (extend-claim! [_ queue run-id seconds] (sql/extend-claim! db queue run-id seconds))
  (set-task-checkpoint-state! [_ queue task-id checkpoint-name value run-id extend-by]
    (sql/set-task-checkpoint-state! db queue task-id checkpoint-name value run-id extend-by))
  (get-task-checkpoint-state [_ queue task-id checkpoint-name]
    (sql/get-task-checkpoint-state db queue task-id checkpoint-name))
  (get-task-checkpoint-states [_ queue task-id run-id]
    (sql/get-task-checkpoint-states db queue task-id run-id))
  (await-event [_ queue task-id run-id checkpoint-name event-name timeout]
    (sql/await-event db queue task-id run-id checkpoint-name event-name timeout))
  (emit-event! [_ queue event-name payload] (sql/emit-event! db queue event-name payload))
  (current-time [_] (sql/current-time db)))

(defn postgres-store
  "A `Store` over a next.jdbc connectable (DataSource or Connection)."
  [connectable]
  (->PostgresStore connectable))
