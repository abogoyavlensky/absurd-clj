(ns quickstart.emit
  "Emit the activation event a provision-user task is waiting for.

  clojure -M:emit [user-id]"
  (:require [absurd.core :as absurd]
            [quickstart.common :as common])
  (:import [java.time Instant]))

(defn -main
  [& [user-id]]
  (let [user-id (or user-id "alice")
        event-name (str "user-activated:" user-id)
        app (common/client)]
    (absurd/emit-event! app event-name {:activated-at (str (Instant/now))})
    (println "emitted" event-name)
    (absurd/close! app)
    (shutdown-agents)))
