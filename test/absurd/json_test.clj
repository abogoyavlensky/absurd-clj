(ns absurd.json-test
  (:require [absurd.json :as json]
            [clojure.test :refer [deftest is testing]])
  (:import [org.postgresql.util PGobject]))

(deftest write-str-keeps-key-names
  (is (= "{\"a-b\":1}" (json/write-str {:a-b 1})))
  (is (= "{\"snake_case\":true}" (json/write-str {"snake_case" true})))
  (is (= "null" (json/write-str nil))))

(deftest read-str-keywordizes-keys
  (is (= {:a 1
          :b-c {:d [1 2]}} (json/read-str "{\"a\":1,\"b-c\":{\"d\":[1,2]}}")))
  (is (nil? (json/read-str "null"))))

(deftest round-trip-normalizes-values
  (testing "keyword values become strings, sets become vectors"
    (is (= {:k "v"
            :s [1]} (json/round-trip {:k :v
                                      :s #{1}}))))
  (testing "nested structures survive"
    (is (= {:a [{:b "c"} 2 nil true]} (json/round-trip {:a [{:b :c} 2 nil true]}))))
  (testing "numbers decode as Long and Double"
    (let [{:keys [i d]} (json/round-trip {:i 42
                                          :d 1.5})]
      (is (instance? Long i))
      (is (instance? Double d))))
  (testing "string keys become keywords"
    (is (= {:x 1} (json/round-trip {"x" 1})))))

(deftest ->jsonb-builds-pgobjects
  (let [^PGobject pg (json/->jsonb {:a 1})]
    (is (= "jsonb" (.getType pg)))
    (is (= "{\"a\":1}" (.getValue pg))))
  (let [^PGobject pg (json/->jsonb nil)]
    (is (= "jsonb" (.getType pg)))
    (is (= "null" (.getValue pg)))))

(deftest <-jsonb-decodes-many-inputs
  (is (= {:a 1} (json/<-jsonb (json/->jsonb {:a 1}))))
  (is (= [1 2] (json/<-jsonb "[1,2]")))
  (is (nil? (json/<-jsonb nil)))
  (is (nil? (json/<-jsonb (json/->jsonb nil)))))
