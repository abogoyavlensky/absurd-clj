(ns app.tasks
  "Task handlers. Handlers are plain functions of params and context."
  (:require [absurd.core :as absurd]
            [clojure.tools.logging :as log])
  (:import [java.time Instant]))

(defn provision-user
  "Create a user, fail once on purpose, send an email, wait for activation."
  [params ctx]
  (let [user (absurd/step ctx "create-user-record"
                          (fn []
                            (log/infof "creating user record for %s" (:user-id params))
                            {:user-id (:user-id params)
                             :email (:email params)
                             :created-at (str (Instant/now))}))
        outage (absurd/begin-step ctx "demo-transient-outage")]
    (when-not (:done? outage)
      (log/warn "simulating a temporary email provider outage")
      (absurd/complete-step ctx outage {:simulated true})
      (throw (ex-info "temporary email provider outage" {:user-id (:user-id user)})))
    (let [delivery (absurd/step ctx "send-activation-email"
                                (fn []
                                  (log/infof "sending activation email to %s" (:email user))
                                  {:sent true
                                   :provider "demo-mail"
                                   :to (:email user)}))
          event-name (str "user-activated:" (:user-id user))
          _ (log/infof "waiting for %s" event-name)
          activation (absurd/await-event ctx event-name {:timeout 3600})]
      {:user-id (:user-id user)
       :email (:email user)
       :delivery delivery
       :status "active"
       :activated-at (:activated-at activation)})))

(defn hello
  "The smallest possible task."
  [params _ctx]
  {:greeting (str "hello, " (:name params "world"))})

(defn register-all!
  "Register every task on the client. Returns the client."
  [app]
  (-> app
      (absurd/register-task! "provision-user" provision-user {:max-attempts 5})
      (absurd/register-task! "hello" hello {:max-attempts 1})))
