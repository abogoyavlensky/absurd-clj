(ns absurd.sqlite.engine
  "The Absurd stored procedures ported to Clojure over SQLite.

  Every procedure is a function over a transaction-scoped connection:
  `(f conn now queue ...)`, where `now` is the engine clock as epoch
  milliseconds, read once per operation the way each PL/pgSQL function reads
  `absurd.current_time()` into `v_now`. Run them inside `with-tx`.

  Values are stored the way the design describes: timestamps as epoch
  milliseconds (`infinity` for Postgres' `'infinity'`), JSON as text, UUIDs as
  text. Procedures take and return the same shapes as `absurd.sql`.

  Errors mirror the PL/pgSQL `raise exception` calls: every one is a
  `java.sql.SQLException` whose message is the Postgres text and whose SQLState
  is AB001, AB002, AB003 or P0001."
  (:require [absurd.json :as json]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import [java.security SecureRandom]
           [java.sql Connection SQLException Statement]
           [java.time Instant]
           [java.util.concurrent.atomic AtomicLong]
           [java.util.concurrent.locks ReentrantLock]
           [javax.sql DataSource]))

;; Errors

(defn raise!
  "Throw an `SQLException` with `message` and `sqlstate`. `sqlstate` defaults
  to P0001, the code of a bare `raise exception`."
  ([message] (raise! "P0001" message))
  ([sqlstate message]
   (throw (SQLException. ^String message ^String sqlstate))))

;; Time

(def infinity
  "The stored value of Postgres' `'infinity'` timestamp."
  Long/MAX_VALUE)

(defn instant->ms
  ^long [^Instant instant]
  (.toEpochMilli instant))

(defn ms->instant
  ^Instant [ms]
  (Instant/ofEpochMilli ms))

;; JSON

(defn encode-json
  "JSON text of `v`. nil is the JSON text `null`; pass nil straight to a
  statement for SQL NULL."
  ^String [v]
  (json/write-str v))

(defn decode-json
  "Decode a JSON column. SQL NULL and the JSON text `null` both give nil."
  [s]
  (some-> s json/read-str))

;; UUIDs

(def ^:private ^SecureRandom secure-random (SecureRandom.))

(def ^:private ^AtomicLong last-uuid-ms (AtomicLong. 0))

(defn uuidv7
  "A UUIDv7 string: 48-bit unix milliseconds, version 7, variant 10 and random
  bits, like `absurd.portable_uuidv7()`. Ids stay strictly increasing within
  the process: when `ms` does not advance past the previous id, the timestamp
  moves one millisecond past it, so `order by run_id` is creation order."
  [ms]
  (let [ts (.updateAndGet last-uuid-ms (reify java.util.function.LongUnaryOperator
                                         (applyAsLong [_ prev] (max (long ms) (inc prev)))))
        rnd (byte-array 10)
        _ (.nextBytes secure-random rnd)
        hi (bit-or (bit-shift-left ts 16)
                   (bit-shift-left 7 12)
                   (bit-and (bit-or (bit-shift-left (bit-and (aget rnd 0) 0xff) 8)
                                    (bit-and (aget rnd 1) 0xff))
                            0x0fff))
        lo (reduce (fn [acc i] (bit-or (bit-shift-left acc 8) (bit-and (aget rnd i) 0xff)))
                   0
                   (range 2 10))
        lo (bit-or (bit-and lo 0x3fffffffffffffff) Long/MIN_VALUE)]
    (str (java.util.UUID. hi lo))))

;; Retry strategy

(def ^:private retry-limit 86400.0)

(defn- json-text
  "The text of a JSON value, like Postgres' `->>`."
  [v]
  (cond
    (nil? v) nil
    (string? v) v
    :else (json/write-str v)))

(defn- number-text->double
  "Parse text as Postgres' `::double precision` would. Unparseable text and
  finite-looking text that overflows raise AB003."
  [^String text]
  (let [t (.trim text)]
    (cond
      (re-matches #"(?i)[-+]?(nan|inf|infinity)" t)
      (Double/parseDouble (-> t (.replaceAll "(?i)infinity|inf" "Infinity") (.replaceAll "(?i)nan" "NaN")))

      (re-matches #"[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?" t)
      (let [d (Double/parseDouble t)
            nonzero-mantissa? (boolean (re-find #"[1-9]" (first (str/split t #"[eE]"))))]
        ;; Postgres rejects overflow and underflow to zero as out of range.
        (if (or (Double/isInfinite d) (and (zero? d) nonzero-mantissa?))
          (raise! "AB003" "retry_strategy contains an invalid number")
          d))

      :else (raise! "AB003" "retry_strategy contains an invalid number"))))

(defn- strategy-number
  [strategy k default]
  (if-let [text (json-text (get strategy k))]
    (number-text->double text)
    default))

(defn retry-delay-seconds
  "Seconds to wait before attempt `attempt + 1`, as `absurd.retry_delay_seconds`:
  a global maximum of one day, explicit invalid values raise AB003, and
  exponential overflow saturates at the configured cap."
  [strategy attempt]
  (let [exponent (max (dec (or attempt 1)) 0)]
    (cond
      (nil? strategy) 0.0

      (not (map? strategy))
      (raise! "AB003" "retry_strategy must be a JSON object")

      :else
      (let [kind (or (json-text (:kind strategy)) "none")]
        (when-not (contains? #{"none" "fixed" "exponential"} kind)
          (raise! "AB003" (str "Unsupported retry strategy kind \"" kind "\"")))
        (if (= "none" kind)
          0.0
          (let [base (strategy-number strategy :base_seconds (if (= "fixed" kind) 60.0 30.0))]
            (when-not (<= 0.0 base retry-limit)
              (raise! "AB003" "retry_strategy.base_seconds must be between 0 and 86400"))
            (if (= "fixed" kind)
              base
              (let [factor (strategy-number strategy :factor 2.0)
                    max-seconds (strategy-number strategy :max_seconds retry-limit)]
                (when (or (neg? factor) (Double/isNaN factor) (Double/isInfinite factor))
                  (raise! "AB003" "retry_strategy.factor must be a finite non-negative number"))
                (when-not (<= 0.0 max-seconds retry-limit)
                  (raise! "AB003" "retry_strategy.max_seconds must be between 0 and 86400"))
                (if (zero? base)
                  0.0
                  (min (* base (Math/pow factor exponent)) max-seconds))))))))))

;; Statements

(def ^:private row-opts
  {:builder-fn rs/as-unqualified-kebab-maps})

(defn query
  "All rows of a select as kebab-case maps."
  [conn sqlvec]
  (jdbc/execute! conn sqlvec row-opts))

(defn query-one
  "The first row of a select, or nil."
  [conn sqlvec]
  (jdbc/execute-one! conn sqlvec row-opts))

(defn update!
  "Run an insert, update or delete and return the number of changed rows."
  [conn sqlvec]
  (long (:next.jdbc/update-count (jdbc/execute-one! conn sqlvec))))

(defn- run-sql!
  [^Connection conn ^String sql]
  (with-open [^Statement st (.createStatement conn)]
    (.execute st sql)))

;; Transactions

(defn- check-autocommit!
  [^Connection conn]
  (when-not (.getAutoCommit conn)
    (throw (ex-info (str "The SQLite connection must have autocommit on: the engine manages "
                         "its own transactions and cannot nest inside one")
                    {:type :absurd/invalid-db}))))

(defn call-in-tx
  "Run `(f conn)` in a transaction on an autocommit connection. `mode` is
  `:write` (`BEGIN IMMEDIATE`, takes the write lock up front so the
  transaction never fails on a later lock upgrade) or `:read` (`BEGIN`).
  Commits when `f` returns and rolls back on any throwable."
  [^Connection conn mode f]
  (run-sql! conn (if (= :read mode) "BEGIN" "BEGIN IMMEDIATE"))
  (let [result (try
                 (f conn)
                 (catch Throwable t
                   (try
                     (run-sql! conn "ROLLBACK")
                     (catch Throwable rollback-failure
                       (.addSuppressed t rollback-failure)))
                   (throw t)))]
    (try
      (run-sql! conn "COMMIT")
      (catch Throwable t
        (try
          (run-sql! conn "ROLLBACK")
          (catch Throwable rollback-failure
            (.addSuppressed t rollback-failure)))
        (throw t)))
    result))

(defn call-with-connection
  "Run `(f conn)` with exclusive use of the environment's connection.

  `env` is `{:connectable c :lock l}`. `c` is a DataSource (a connection is
  borrowed and closed afterwards) or a Connection (used as is, never closed).
  The in-process `ReentrantLock` serializes operations from threads that share
  the environment, which is what makes one shared Connection safe."
  [{:keys [connectable ^ReentrantLock lock]} f]
  (.lock lock)
  (try
    (if (instance? DataSource connectable)
      (with-open [^Connection conn (.getConnection ^DataSource connectable)]
        (check-autocommit! conn)
        (f conn))
      (do (check-autocommit! connectable)
          (f connectable)))
    (finally
      (.unlock lock))))

(defn run-tx
  "Run `(f conn)` in a transaction on the environment's connection. See
  `call-with-connection` and `call-in-tx`."
  [env mode f]
  (call-with-connection env #(call-in-tx % mode f)))

(defmacro with-tx
  "Run `body` with `conn` bound to the environment's connection inside a write
  transaction (`BEGIN IMMEDIATE`):

      (with-tx [conn env] ...)"
  [[conn env] & body]
  `(run-tx ~env :write (fn [~conn] ~@body)))

(defmacro with-read-tx
  "Like `with-tx`, for operations that only read (a deferred `BEGIN`)."
  [[conn env] & body]
  `(run-tx ~env :read (fn [~conn] ~@body)))
