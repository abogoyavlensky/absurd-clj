(ns quickstart.common
  (:require [absurd.core :as absurd]))

(def default-url "postgresql://absurd:absurd@localhost:5433/absurd")

(defn client
  "A client on the `default` queue, connected through ABSURD_DATABASE_URL or
  the examples compose stack."
  []
  (absurd/client {:db (or (System/getenv "ABSURD_DATABASE_URL") default-url)
                  :queue "default"}))
