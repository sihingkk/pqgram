(ns plans
  (:require [pqgram :refer [pq-gram-distance]]
            [neo4j-clj.core :as db]
            [clojure.java.io :refer [make-parents]]
            [pqgram.db :refer [local-db]]
            [clojure.zip :as z]))

(db/defquery macro-group-names-query
  "MATCH (group:BenchmarkGroup)-[]-(tool:BenchmarkTool {name: \"macro\"})
   RETURN group.name as group")


(defn macro-group-names []
  (with-open [session (db/get-session local-db)]
    (doall  (mapv :group (macro-group-names-query session {})))))


(db/defquery hash->benchmark-query
  "MATCH (m:Metrics)-[:METRICS_FOR]->(b:Benchmark )<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup),
   (m:Metrics)-[:HAS_PLAN]->(plan:Plan)-[:HAS_PLAN_TREE]->(tree:PlanTree {description_hash: $hash})
   RETURN b.name as name, bg.name as group")


(defn hash->benchmark [hash]
  (with-open [session (db/get-session local-db)]
    (doall  (mapv :name (hash->benchmark-query session {:hash hash})))))

(db/defquery tree-hash
  "MATCH (m:Metrics)-[:METRICS_FOR]->(b:Benchmark )<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup {name: $name}),
         (m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(p:Project {commit: $commit}),
   (m:Metrics)-[:HAS_PLAN]->(plan:Plan)-[:HAS_PLAN_TREE]->(tree:PlanTree)
   RETURN   tree.description_hash as id")


(defn find-tree-hash [{:keys [commit group-name]}]
  (with-open [session (db/get-session local-db)]
    (doall  (mapv :id (tree-hash session {:commit commit :name group-name})))))

(defn tree-zipper [tree]
  (z/zipper vector?
            rest
            (fn [[x _ _] children]
              (vec (cons x children)))
            tree))

(def d (pq-gram-distance tree-zipper 3 2))

(db/defquery operator-paths
  "MATCH (tree:PlanTree {description_hash: $id})-[:HAS_OPERATORS]->(root:Operator),
   p = (root)-[:HAS_CHILD*]->(operator:Operator)
   RETURN p")


(defn find-operator-paths [hash]
  (with-open [session (db/get-session local-db)]
    (->> (operator-paths session {:id hash})
         (doall)
         (mapv :p))))

(defn ->node [node-value]
  (.asNode (.asValue node-value)))

(defn ->id [node]
  (.id node))

(defn ->clj [node]
  {:id (.id node)
   :name (-> node
             (.asMap)
             (get "operator_type"))})

(defn seg->pair [seg]
  (let [start (->node (.start seg))
        end (->node (.end seg))]
    [(->clj start)
     (->clj end)]))

(defn path->pair [path]
  (->> path
       (.asPath)
       (map seg->pair)))

(defn find-children  [coll node]
  (->> coll
       (filter (fn [[parent _]] (= parent node)))
       (map second)))


(defn ->tree'
  ([db node]
   (->tree' db node []))

  ([db node acc]
   (let [children (find-children db node)
         name (:name node)]
     (if (empty? children)
       name
       (vec (concat acc [name] (mapv #(->tree' db %) children)))))))

(defn plan-hash->tree [plan-hash]
  (let [paths (find-operator-paths plan-hash)
        db  (->> paths
                 (mapcat path->pair)
                 (distinct))]
    [plan-hash (->tree' db (ffirst db))]))


(db/defquery query-plan-txt
  "MATCH (tree:PlanTree {description_hash: $hash})
   
   RETURN tree.description as description")

(defn find-query-plan-txt [hash]
  (with-open [session (db/get-session local-db)]
    (first  (map :description  (query-plan-txt session {:hash hash})))))

(defn top-similar [n trees]
  (for [[plan-hash left] trees]
    {:plan-hash plan-hash
     :similar (->> trees
                   (map
                    (fn [[plan-hash right]]
                      [plan-hash (d left right)]))
                   (sort-by second)
                   (reverse)
                   (take n)
                   (filter #(not= (first %) plan-hash)))}))


(defn num->str [num]
  (let [s (str (double num))]
    (if (> 4 (count s))
      s
      (subs s 0 4))))

(defn spit-parent [group  left-hash]
  (let [file-name (str "/Users/chrisk/temp/" group "/" left-hash "/" "__" left-hash ".txt")]
    (make-parents file-name)
    (spit file-name (find-query-plan-txt left-hash))))


(defn spit-child [group similarity left-hash right-hash]
  (let [file-name (str "/Users/chrisk/temp/" group "/" left-hash "/" (num->str similarity) "__" right-hash ".txt")]
    (make-parents file-name)
    (spit file-name (find-query-plan-txt right-hash))))


(defn run! []
  (doseq [group-name (macro-group-names)]
    (println "processing " group-name)
    (let [result
          (->> {:commit "0b7d6e27a7cd0534b1a9f27607b1b99ab6444774"
                :group-name group-name}
               (find-tree-hash)
               (map plan-hash->tree)
               (top-similar 100))]

      (doseq [{:keys [plan-hash similar]} result]
        (spit-parent group-name plan-hash)
        (doseq [[hash similarity] similar]
          (spit-child group-name similarity plan-hash hash))))))
