(ns absurd.determinism-test
  "Property: a step's return value on attempt one equals the value a replay
  observes, for any JSON-shaped Clojure value."
  (:require [absurd.core :as absurd]
            [absurd.json :as json]
            [absurd.test-helpers :as h]
            [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))

(use-fixtures :each h/with-conn)

(def ^:private safe-string
  "Strings Postgres jsonb accepts: no NUL characters, no lone surrogates."
  (gen/one-of [gen/string-ascii
               (gen/fmap #(apply str %)
                         (gen/vector (gen/fmap char (gen/choose 0x20 0xD7FF)) 0 8))]))

(def ^:private scalar
  (gen/one-of [gen/boolean
               gen/large-integer
               (gen/double* {:NaN? false
                             :infinite? false})
               safe-string
               gen/keyword
               (gen/return nil)]))

(def ^:private json-value
  (gen/recursive-gen
    (fn [inner]
      (gen/one-of [(gen/vector inner 0 4)
                   (gen/map (gen/one-of [gen/keyword gen/string-alphanumeric]) inner {:max-elements 4})]))
    scalar))

(deftest step-values-are-identical-across-replay
  (let [app (h/test-client)
        generated (atom nil)
        first-seen (atom ::none)
        second-seen (atom ::none)
        _ (absurd/register-task! app "replay"
                                 (fn [_ ctx]
                                   (let [value (absurd/step ctx "value" (fn [] @generated))]
                                     (if (= 1 (:attempt ctx))
                                       (do (reset! first-seen value)
                                           (throw (ex-info "retry to force a replay" {})))
                                       (do (reset! second-seen value)
                                           nil))))
                                 {:max-attempts 2})
        property (prop/for-all [value json-value]
                               (reset! generated value)
                               (reset! first-seen ::none)
                               (reset! second-seen ::none)
                               (let [spawned (absurd/spawn! app "replay" nil)]
                                 (absurd/work-batch! app {:worker-id "replay-worker"})
                                 (absurd/work-batch! app {:worker-id "replay-worker"})
                                 (and (= :completed (:state (absurd/fetch-task-result app (:task-id spawned))))
                                      (= @first-seen @second-seen)
                                      (= @first-seen (json/round-trip value)))))
        result (tc/quick-check 20 property)]
    (is (true? (:pass? result))
        (pr-str (select-keys result [:fail :shrunk :num-tests])))))
