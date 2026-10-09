(ns absurd-clj.core-test
  (:require [absurd-clj.core :as core]
            [clojure.test :refer :all]))

(deftest test-sum
  (is (= 3 (core/sum 1 2))))
