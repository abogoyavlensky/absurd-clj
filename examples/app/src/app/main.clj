(ns app.main
  "CLI entry point.

  clojure -M:run worker                 run the worker until Ctrl-C
  clojure -M:run spawn <user-id> [email] spawn a provision-user task
  clojure -M:run activate <user-id>     emit the activation event
  clojure -M:run result <task-id>       print a task's current snapshot
  clojure -M:run hello [name]           spawn a hello task and wait for it"
  (:require [absurd.core :as absurd]
            [app.system :as system]
            [clojure.tools.logging :as log])
  (:import [java.time Instant])
  (:gen-class))

(defn- with-client
  "Run `f` with a started client (pool + client, no worker), then stop."
  [f]
  (let [system (system/start! [:app/absurd])]
    (try
      (f (:app/absurd system))
      (finally
        (system/stop! system)))))

(defn- run-worker!
  []
  (let [system (system/start! [:app/worker])]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable (fn [] (system/stop! system))))
    (log/info "worker running on queue" (:queue (:app/absurd system)) "; press Ctrl-C to stop")
    @(promise)))

(defn- spawn!
  [user-id email]
  (with-client
    (fn [app]
      (let [spawned (absurd/spawn! app "provision-user" {:user-id user-id
                                                         :email email})]
        (println "spawned:" spawned)
        (println "snapshot:" (absurd/fetch-task-result app (:task-id spawned)))))))

(defn- activate!
  [user-id]
  (with-client
    (fn [app]
      (let [event-name (str "user-activated:" user-id)]
        (absurd/emit-event! app event-name {:activated-at (str (Instant/now))})
        (println "emitted" event-name)))))

(defn- result
  [task-id]
  (with-client
    (fn [app]
      (println "snapshot:" (absurd/fetch-task-result app task-id)))))

(defn- hello!
  [who]
  (with-client
    (fn [app]
      (let [spawned (absurd/spawn! app "hello" {:name who})]
        (println "spawned:" (:task-id spawned))
        (println "result:" (absurd/await-task-result app (:task-id spawned) {:timeout 30}))))))

(defn -main
  [& [command & args]]
  (case command
    (nil "worker") (run-worker!)
    "spawn" (let [[user-id email] args
                  user-id (or user-id "alice")]
              (spawn! user-id (or email (str user-id "@example.com"))))
    "activate" (activate! (or (first args) "alice"))
    "result" (if-let [task-id (first args)]
               (result task-id)
               (do (println "usage: result <task-id>") (System/exit 2)))
    "hello" (hello! (or (first args) "world"))
    (do (println (:doc (meta (find-ns 'app.main))))
        (System/exit 2)))
  (shutdown-agents)
  (System/exit 0))
