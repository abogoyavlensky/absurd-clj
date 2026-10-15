(ns absurd.schema-test
  (:require [absurd.core :as absurd]
            [absurd.sqlite.schema :as schema]
            [absurd.test-helpers :as h]
            [clojure.test :refer [deftest is use-fixtures]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(use-fixtures :each h/with-conn)

(deftest vendored-schema-version-matches-target
  (h/only :postgres
          (is (= "0.5.0"
                 (:v (jdbc/execute-one! h/*conn* ["select absurd.get_schema_version() as v"]))))))

(deftest sqlite-schema-version-is-recorded
  (h/only :sqlite
          (absurd/list-queues (absurd/client (h/client-opts)))
          (is (= schema/schema-version
                 (:value (jdbc/execute-one! h/*conn* ["select value from absurd_meta where key = 'schema_version'"]
                                            {:builder-fn rs/as-unqualified-kebab-maps}))))))
