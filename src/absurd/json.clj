(ns absurd.json
  "JSON codec shared by the SDK.

  Keys are keywordized on decode and written by name on encode. Step values
  and task results pass through `round-trip` before they are returned, so
  attempt one and replay see the same shape."
  (:require [jsonista.core :as json])
  (:import [com.fasterxml.jackson.databind DeserializationFeature ObjectMapper]
           [org.postgresql.util PGobject]))

(def ^:private ^ObjectMapper mapper
  (doto (json/object-mapper {:decode-key-fn keyword
                             :encode-key-fn name})
    ;; Integers decode as Long, matching every other Clojure JSON library.
    (.configure DeserializationFeature/USE_LONG_FOR_INTS true)))

(defn write-str
  "Encode a Clojure value as a JSON string. nil encodes as `null`."
  ^String [v]
  (json/write-value-as-string v mapper))

(defn read-str
  "Decode a JSON string into Clojure data with keyword keys."
  [^String s]
  (json/read-value s mapper))

(defn round-trip
  "Encode then decode, yielding the shape a replay would observe."
  [v]
  (read-str (write-str v)))

(defn ->jsonb
  "Wrap a value as a `jsonb` PGobject parameter."
  ^PGobject [v]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (write-str v))))

(defn <-jsonb
  "Decode a `json`/`jsonb` PGobject, a JSON string, or nil."
  [v]
  (cond
    (nil? v) nil
    (instance? PGobject v) (some-> (.getValue ^PGobject v) read-str)
    (string? v) (read-str v)
    :else v))
