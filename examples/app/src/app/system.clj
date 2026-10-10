(ns app.system
  "Integrant wiring: connection pool, Absurd client, worker."
  (:require [absurd.core :as absurd]
            [aero.core :as aero]
            [app.tasks :as tasks]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [next.jdbc.connection :as connection])
  (:import [com.zaxxer.hikari HikariDataSource]))

(defmethod aero/reader 'ig/ref
  [_ _ value]
  (ig/ref value))

(defn config
  []
  (aero/read-config (io/resource "config.edn")))

(defmethod ig/init-key :app/db
  [_ {:keys [jdbc-url maximum-pool-size]}]
  (connection/->pool HikariDataSource {:jdbcUrl jdbc-url
                                       :maximumPoolSize maximum-pool-size}))

(defmethod ig/halt-key! :app/db
  [_ ^HikariDataSource pool]
  (.close pool))

(defn- wrap-task-execution
  "Log around every task run. The hook must return `(execute)` and must let
  exceptions through: the SDK suspends tasks by throwing."
  [ctx execute]
  (log/infof "task %s (%s) attempt %d started" (:task-name ctx) (:task-id ctx) (:attempt ctx))
  (try
    (let [result (execute)]
      (log/infof "task %s (%s) completed" (:task-name ctx) (:task-id ctx))
      result)
    (catch Throwable t
      (log/infof "task %s (%s) left with %s" (:task-name ctx) (:task-id ctx)
                 (or (:type (ex-data t)) (.getName (class t))))
      (throw t))))

(defmethod ig/init-key :app/absurd
  [_ {:keys [db queue max-attempts]}]
  (-> (absurd/client {:db db
                      :queue queue
                      :max-attempts max-attempts
                      :hooks {:wrap-task-execution wrap-task-execution
                              :on-lease-timeout (fn [ctx seconds]
                                                  (log/warnf "task %s ran past its %ss claim"
                                                             (:task-id ctx) seconds))}})
      tasks/register-all!))

(defmethod ig/init-key :app/worker
  [_ {:keys [absurd]
      :as opts}]
  (absurd/start-worker! absurd (select-keys opts [:worker-id :concurrency :claim-timeout :poll-interval])))

(defmethod ig/halt-key! :app/worker
  [_ worker]
  (absurd/stop-worker! worker))

(defn start!
  "Start the system, or only the given component keys and their dependencies."
  ([] (ig/init (config)))
  ([component-keys] (ig/init (config) component-keys)))

(defn stop!
  [system]
  (ig/halt! system))
