(ns mail-archive.datascript-store
  "The DataScript backend of the `Store` — real DataScript via the npm `datascript`
  package, reachable only from nbb. This is the browser-native / zero-JVM query
  surface that must return the SAME answers as `mail_archive.store/LangchainDbStore`
  against the shared schema (`mail_archive/schema.edn`); the twin contract tests
  (`store_contract_test.cljc` on JVM, `datascript_contract_test.cljs` here) prove it.

  Pragmatic deviation (documented per the design): nbb's SCI loader can't cleanly
  `:require` a JVM `.cljc` protocol namespace, so this file does NOT reify
  `mail_archive.store/Store`; it implements the same three operations
  (`transact!`/`q`/`pull`) as plain top-level fns over a store map
  `{:conn <ds-conn> :schema <schema-map>}`. Same shape, no protocol import.

  Two-pass ref flattening (the crux): the established nbb+datascript.js pattern
  (`manifest/edn-query.cljs`) marshals cljs maps to JS objects and stringifies any
  nested map (`(map? v) (pr-str v)`) — i.e. DataScript's own nested-map ref
  expansion is unreachable through the JS interface. So `transact!` here does its
  OWN expansion: it walks the tx-data, hoists every nested map under a ref attr
  into its own entity with a fresh negative tempid, and replaces the nested map
  with that tempid (a scalar the JS layer passes through untouched). Unique-identity
  upsert then unifies tempids carrying the same `:person/id`/`:blob/cid` into one
  entity — exactly as `langchain.db` does — so refs resolve identically on both
  sides. What we hand `(.transact ds conn ...)` is only flat JS objects: `:db/id` +
  bare-string attrs whose values are scalars, tempid ints, or arrays thereof."
  (:require ["datascript" :as ds-mod]
            [clojure.edn :as edn]
            ["fs" :as fs]))

(def ds (.-default ds-mod))

;; ───────────────────────── schema (map -> datascript.js schema) ─────────────────────────

(defn kw->attr
  "Keyword -> the bare string datascript.js uses for attributes (no leading colon)."
  [k]
  (cond
    (keyword? k) (if-let [ns (namespace k)] (str ns "/" (name k)) (name k))
    :else (str k)))

(defn ref-attr? [schema a] (= :db.type/ref (get-in schema [a :db/valueType])))
(defn card-many? [schema a] (= :db.cardinality/many (get-in schema [a :db/cardinality])))

(defn ds-schema
  "Translate the shared schema *map* into a datascript.js schema JS object. Only
  ref/cardinality/uniqueness constraints matter to DataScript; attribute keys and
  the `:db/*` value keywords are the bare/`:`-prefixed strings datascript.js wants
  (same convention as manifest/edn-query.cljs)."
  [schema]
  (let [obj (js-obj)]
    (doseq [[attr props] schema]
      (let [spec (js-obj)]
        (when (:db/unique props) (aset spec ":db/unique" ":db.unique/identity"))
        (when (= :db.type/ref (:db/valueType props)) (aset spec ":db/valueType" ":db.type/ref"))
        (when (= :db.cardinality/many (:db/cardinality props))
          (aset spec ":db/cardinality" ":db.cardinality/many"))
        (when (pos? (alength (js-keys spec)))
          (aset obj (kw->attr attr) spec))))
    obj))

;; ───────────────────────── two-pass ref flattening ─────────────────────────

(defn flatten-tx
  "Walk nested-map tx-data (the SAME shape LangchainDbStore accepts) and return a
  flat vector of entity maps (keyword attrs; `:db/id` = a fresh negative tempid or
  the caller's explicit id): every nested map under a ref attr is hoisted to its
  own entity and replaced by that entity's tempid; card-many ref values become a
  vector of tempids. No nested maps remain."
  [schema tx-data]
  (let [counter (atom 0)
        out (atom [])
        walk (fn walk [m]
               (let [id (or (:db/id m) (swap! counter dec))
                     flat (reduce-kv
                           (fn [acc a v]
                             (cond
                               (and (ref-attr? schema a) (map? v))
                               (assoc acc a (walk v))
                               (and (ref-attr? schema a) (sequential? v))
                               (assoc acc a (mapv (fn [x] (if (map? x) (walk x) x)) v))
                               :else (assoc acc a v)))
                           {:db/id id}
                           (dissoc m :db/id))]
                 (swap! out conj flat)
                 id))]
    (doseq [m tx-data] (walk m))
    @out))

(defn ->js-entity
  "Flat entity map (keyword attrs; scalar / tempid-int / vector values) -> the JS
  object datascript.js transacts: `:db/id` under the literal key \":db/id\", other
  attrs under bare-string keys, vectors as JS arrays, keywords stringified."
  [m]
  (let [obj (js-obj)]
    (doseq [[k v] m]
      (aset obj (if (= k :db/id) ":db/id" (kw->attr k))
            (cond
              (vector? v) (into-array (map #(if (keyword? %) (kw->attr %) %) v))
              (keyword? v) (kw->attr v)
              :else v)))
    obj))

;; ───────────────────────── store ops (Store shape, plain fns) ─────────────────────────

(defn create-store
  "A DataScript-backed store map `{:conn :schema}` over the shared schema."
  [schema]
  {:conn (.create_conn ds (ds-schema schema)) :schema schema})

(defn transact!
  "Transact nested-map tx-data (flattened via `flatten-tx`) into DataScript."
  [store tx-data]
  (.transact ds (:conn store) (into-array (map ->js-entity (flatten-tx (:schema store) tx-data)))))

(defn q
  "Datalog query (an EDN string, bare-string attrs) against the store's db, plus
  optional extra `:in` inputs. Returns the js->clj'd result."
  [store query & inputs]
  (js->clj (apply (.-q ds) query (.db ds (:conn store)) (map clj->js inputs))))

(defn pull
  "Datomic-style pull (EDN-string pattern) of entity `eid`."
  [store pattern eid]
  (js->clj (.pull ds (.db ds (:conn store)) pattern eid)))

;; ───────────────────────── schema loading (nbb) ─────────────────────────

(defn load-schema
  "Read the shared schema map from `src/mail_archive/schema.edn` (relative to the
  repo root, the cwd nbb runs from)."
  ([] (load-schema "src/mail_archive/schema.edn"))
  ([path] (edn/read-string (.readFileSync fs path "utf8"))))
