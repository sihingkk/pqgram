(ns plans
  (:require [pqgram :refer [pq-gram-distance]]
            [neo4j-clj.core :as db]
            [clojure.java.io :refer [make-parents]]
            [pqgram.db :refer [local-db]]
            [clojure.zip :as z]))

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

(comment (find-tree-hash  {:commit "0b7d6e27a7cd0534b1a9f27607b1b99ab6444774"
                           :group-name "ldbc_sf010"}))

(defn find-test-run-ids [{:keys [commit group-name]}]
  (with-open [session (db/get-session local-db)]
    (doall  (mapv :id (test-run-ids session {:commit commit :name group-name})))))

(defn tree-zipper [tree]
  (z/zipper vector?
            rest
            (fn [[x _ _] children]
              (vec (cons x children)))
            tree))

(def d (pq-gram-distance tree-zipper 3 2))

(def example-id
  (first (find-test-run-ids {:commit "0b7d6e27a7cd0534b1a9f27607b1b99ab6444774"
                             :group-name "accesscontrol"})))


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

(defn test-run-id->tree [test-run-id]
  (let [paths (find-operator-paths test-run-id)
        db  (->> paths
                 (mapcat path->pair)
                 (distinct))]
    [test-run-id (->tree' db (ffirst db))]))


(db/defquery query-plan-txt
  "MATCH (tree:PlanTree {description_hash: $hash})
   
   RETURN tree.description as description")

(defn find-query-plan-txt [hash]
  (with-open [session (db/get-session local-db)]
    (first  (map :description  (query-plan-txt session {:hash hash})))))

(defn top-similar [n tree-hashes]
  (doall (let [trees  (map test-run-id->tree tree-hashes)]
           (println "count of trees: " (count trees))
           (for [[test-run-id left] trees]
             {:test-run-id test-run-id
              :similar (->> trees
                            (map
                             (fn [[test-run-id right]]
                               [test-run-id (d left right)]))
                            (sort-by second)
                            (reverse)
                            #_(filter #(not= (second %) 1))
                            (take n)
                            (filter #(not= (first %) test-run-id)))}))))


(defn num->str [num]
  (let [s (str (double num))]
    (if (> 4 (count s))  
      s
      (subs s 0 4)
      )))

(defn spit-parent [group  left-hash]
  (let [file-name (str "/Users/chrisk/temp/" group "/" left-hash "/" "__" left-hash ".txt")]
    (make-parents file-name)
    (spit file-name (find-query-plan-txt left-hash))))


(defn spit-child [group similarity left-hash right-hash]
  (let [file-name (str "/Users/chrisk/temp/" group "/" left-hash "/" (num->str similarity) "__" right-hash ".txt")]
    (make-parents file-name)
    (spit file-name (find-query-plan-txt right-hash))))

(def groups [
"logistics"
"grid"
"nexlp"
"levelstory"
"elections"
"cineasts"
"accesscontrol"
"recommendations"
"socialnetwork"
"index_backed_order_by"
"bubble_eye"
"musicbrainz"
"qmul_read"
"qmul_write"
"ldbc_sf001"
"ldbc_sf010"
"pokec_read"
"generatedmusicdata_read"
"osmnodes"
"cineasts_csv"
"generatedmusicdata_write"
"pokec_write"
"generated_queries"
"ldbc_ish"
"zero"
"ldbc_ish_sf010"
"offshore_leaks"
"alacrity"
"fraud-poc-credit"
"fraud-poc-aml"
"ciena"             
           ])

(comment
  (doseq [group-name groups]
    (println "processing " group-name)
    (let [result (top-similar 100 (find-tree-hash {:commit "0b7d6e27a7cd0534b1a9f27607b1b99ab6444774"
                                                   :group-name group-name}))]
      
      (doseq [{:keys [test-run-id similar]} result]
        (spit-parent group-name test-run-id)
        (doseq [[hash similarity] similar]
          (spit-child group-name similarity test-run-id hash)) 
        )))
  
  )

(spit-parent "ldbc" "92d6eb754c3825cc29f79e1a41edc7d350860b8e74a6ae37a18306a027cc0759")


(spit-child "ldbc" 59/60 "92d6eb754c3825cc29f79e1a41edc7d350860b8e74a6ae37a18306a027cc0759" "993081a2933245aab57002e25b41fce0a76e3e0599b0d8e3772a0750be893b48")
