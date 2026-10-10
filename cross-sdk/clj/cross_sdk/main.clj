(ns cross-sdk.main
  "Clojure side of the cross-SDK check.

  Forward: spawn `py-echo` on `xsdk_py`, worked by the Python SDK, and check
  its result. Reverse: keep a worker on `xsdk_clj` alive until the Python
  client has run `clj-echo` and emitted `xsdk:done`."
  (:require [absurd.core :as absurd])
  (:gen-class))

(defn- register-tasks!
  [app]
  (-> app
      (absurd/register-task! "clj-echo"
                             (fn [params ctx]
                               (let [step (absurd/step ctx "step" (fn [] {:seen params}))]
                                 (absurd/sleep-for ctx "nap" 1)
                                 (let [go (absurd/await-event ctx (str "go:" (:task-id ctx)) {:timeout 60})]
                                   {:from "clojure" :params params :step step :go go})))
                             {:max-attempts 1})
      (absurd/register-task! "wait-done"
                             (fn [_ ctx] (absurd/await-event ctx "xsdk:done" {:timeout 180}))
                             {:max-attempts 1})))

(defn- forward-check!
  [app]
  (let [spawned (absurd/spawn! app "py-echo" {:n 1 :who "clojure"} {:queue "xsdk_py"})]
    (absurd/emit-event! app (str "go:" (:task-id spawned)) {:ok true} {:queue "xsdk_py"})
    (let [result (absurd/await-task-result app (:task-id spawned) {:queue "xsdk_py" :timeout 90})]
      (println "clojure->python:" (:state result) (pr-str (:result result)))
      (and (= :completed (:state result))
           (= "python" (get-in result [:result :from]))
           (= {:n 1 :who "clojure"} (get-in result [:result :params]))
           (= {:ok true} (get-in result [:result :go]))))))

(defn- reverse-check!
  [app]
  (let [spawned (absurd/spawn! app "wait-done" nil)
        result (absurd/await-task-result app (:task-id spawned) {:timeout 180})]
    (println "python->clojure signal:" (:state result) (pr-str (:result result)))
    (and (= :completed (:state result))
         (true? (get-in result [:result :ok])))))

(defn -main
  [& _]
  (let [app (absurd/client {:db (System/getenv "ABSURD_DATABASE_URL") :queue "xsdk_clj"})
        _ (register-tasks! app)
        ok? (with-open [_ (absurd/start-worker! app {:worker-id "clj-worker"
                                                     :claim-timeout 30
                                                     :poll-interval 100})]
              (let [forward-ok? (try (forward-check! app)
                                     (catch Throwable t
                                       (println "clojure->python: error" (ex-message t))
                                       false))
                    reverse-ok? (try (reverse-check! app)
                                     (catch Throwable t
                                       (println "python->clojure: error" (ex-message t))
                                       false))]
                (and forward-ok? reverse-ok?)))]
    (println (if ok? "cross-sdk: OK" "cross-sdk: FAILED"))
    (shutdown-agents)
    (System/exit (if ok? 0 1))))
