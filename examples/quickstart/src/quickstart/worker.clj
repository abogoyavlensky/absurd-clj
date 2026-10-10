(ns quickstart.worker
  "Durable worker: steps, a decomposed step that fails once, and an event wait."
  (:require [absurd.core :as absurd]
            [quickstart.common :as common])
  (:import [java.time Instant]))

(defn provision-user
  [params ctx]
  (let [user (absurd/step ctx "create-user-record"
                          (fn []
                            (println (:task-id ctx) "creating user record for" (:user-id params))
                            {:user-id (:user-id params)
                             :email (:email params)
                             :created-at (str (Instant/now))}))
        ;; Demo only: fail once after the first checkpoint so the retry is visible.
        outage (absurd/begin-step ctx "demo-transient-outage")]
    (when-not (:done? outage)
      (println (:task-id ctx) "simulating a temporary email provider outage")
      (absurd/complete-step ctx outage {:simulated true})
      (throw (ex-info "temporary email provider outage" {})))
    (let [delivery (absurd/step ctx "send-activation-email"
                                (fn []
                                  (println (:task-id ctx) "sending activation email to" (:email user))
                                  {:sent true
                                   :provider "demo-mail"
                                   :to (:email user)}))
          event-name (str "user-activated:" (:user-id user))
          _ (println (:task-id ctx) "waiting for" event-name)
          activation (absurd/await-event ctx event-name {:timeout 3600})]
      {:user-id (:user-id user)
       :email (:email user)
       :delivery delivery
       :status "active"
       :activated-at (:activated-at activation)})))

(defn -main
  [& _]
  (let [app (absurd/register-task! (common/client) "provision-user" provision-user {:max-attempts 5})
        worker (absurd/start-worker! app {:concurrency 2
                                          :poll-interval 250})]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable (fn [] (absurd/stop-worker! worker))))
    (println "worker listening on queue default")
    @(promise)))
