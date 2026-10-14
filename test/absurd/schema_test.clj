(ns absurd.schema-test
  (:require [absurd.test-helpers :as h]
            [clojure.test :refer [deftest is use-fixtures]]
            [next.jdbc :as jdbc]))

(use-fixtures :each h/with-conn)

(deftest vendored-schema-version-matches-target
  (h/only :postgres
          (is (= "0.5.0"
                 (:v (jdbc/execute-one! h/*conn* ["select absurd.get_schema_version() as v"]))))))
