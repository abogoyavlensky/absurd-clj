(ns absurd.json
  "JSON codec shared by the SDK.

  Keys are keywordized on decode and written by name on encode. Step values
  and task results pass through `round-trip` before they are returned, so
  attempt one and replay see the same shape.

  On the JVM the codec is jsonista and jsonb parameters are `PGobject`s; under
  let-go it is the built-in `json` namespace and jsonb travels as a string
  with an explicit `::jsonb` cast in the SQL."
  #?(:lg nil :clj (:require [jsonista.core :as jsonista]))
  #?(:lg nil :clj (:import [com.fasterxml.jackson.databind DeserializationFeature ObjectMapper]
                           [org.postgresql.util PGobject])))

#?(:lg nil :clj
   (def ^:private ^ObjectMapper mapper
     (doto (jsonista/object-mapper {:decode-key-fn keyword
                                    :encode-key-fn name})
       ;; Integers decode as Long, matching every other Clojure JSON library.
       (.configure DeserializationFeature/USE_LONG_FOR_INTS true))))

(defn write-str
  "Encode a Clojure value as a JSON string. nil encodes as `null`."
  ^String [v]
  #?(:lg (json/write-json v)
     :clj (jsonista/write-value-as-string v mapper)))

(defn read-str
  "Decode a JSON string into Clojure data with keyword keys."
  [^String s]
  #?(:lg (json/read-json s {:keywords? true})
     :clj (jsonista/read-value s mapper)))

(defn round-trip
  "Encode then decode, yielding the shape a replay would observe."
  [v]
  (read-str (write-str v)))

(defn ->jsonb
  "Wrap a value as a `jsonb` parameter: a `PGobject` on the JVM, a JSON
  string under let-go (the SQL casts it with `::jsonb`)."
  [v]
  #?(:lg (write-str v)
     :clj (doto (PGobject.)
            (.setType "jsonb")
            (.setValue (write-str v)))))

(defn <-jsonb
  "Decode a `json`/`jsonb` column value (a `PGobject`, a JSON string) or nil."
  [v]
  (cond
    (nil? v) nil
    #?@(:lg [] :clj [(instance? PGobject v) (some-> (.getValue ^PGobject v) read-str)])
    (string? v) (read-str v)
    :else v))
