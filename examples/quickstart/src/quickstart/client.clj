(ns quickstart.client
  "Spawn a provision-user task and optionally wait for its result.

  clojure -M:client [--await] [user-id] [email]"
  (:require [absurd.core :as absurd]
            [quickstart.common :as common]))

(defn -main
  [& args]
  (let [await? (some #{"--await"} args)
        [user-id email] (remove #{"--await"} args)
        user-id (or user-id "alice")
        email (or email (str user-id "@example.com"))
        app (common/client)
        spawned (absurd/spawn! app "provision-user" {:user-id user-id
                                                     :email email} {:queue "default"})]
    (println "spawned:" spawned)
    (println "current snapshot:" (absurd/fetch-task-result app (:task-id spawned)))
    (when await?
      (println (str "waiting for completion; emit user-activated:" user-id " on queue default"))
      (println "final snapshot:" (absurd/await-task-result app (:task-id spawned) {:timeout 300})))
    (absurd/close! app)
    (shutdown-agents)))
