(ns plans
  (:require [pqgram :refer [pq-gram-distance]]
            [neo4j-clj.core :as db]
            [pqgram.db :refer [local-db]]
            [clojure.zip :as z]))

(db/defquery test-run-ids
  "MATCH (m:Metrics)-[:METRICS_FOR]->(b:Benchmark )<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup {name: $name}),
         (m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(p:Project {commit: $commit})
   RETURN tr.id as id")

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
  "MATCH (testRun:TestRun {id: $id})-[:HAS_METRICS]->(metrics:Metrics)-[:HAS_PLAN]->(plan:Plan),
   (plan)-[:HAS_PLAN_TREE]->(tree:PlanTree)-[:HAS_OPERATORS]->(root:Operator),
   p = (root)-[:HAS_CHILD*]->(operator:Operator)
   RETURN p")


(defn find-operator-paths [id]
  (with-open [session (db/get-session local-db)]
    (->> (operator-paths session {:id id})
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
  "MATCH (testRun:TestRun {id: $id})-[:HAS_METRICS]->(metrics:Metrics)-[:HAS_PLAN]->(plan:Plan),
   (plan)-[:HAS_PLAN_TREE]->(tree:PlanTree)
   
   RETURN tree.description as description")

(defn find-query-plan-txt [test-run-id]
  (with-open [session (db/get-session local-db)]
    (first  (map :description  (query-plan-txt session {:id test-run-id})))))

(comment
  (time (take 3 (doall (let [trees  (map test-run-id->tree (find-test-run-ids {:commit "0b7d6e27a7cd0534b1a9f27607b1b99ab6444774"
                                                                               :group-name "accesscontrol"}))]
                         (println "count of trees: " (count trees))
                         (for [[test-run-id left] trees]
                           {:test-run-id test-run-id
                            :similar (->> trees
                                          (map
                                           (fn [[test-run-id right]]
                                             [test-run-id (d left right)]))
                                          (sort-by second)
                                          (reverse)
                                          (filter #(not= (second %) 1))
                                          (take 5)
                                          (filter #(not= (first %) test-run-id)))})))))
  )
(comment  
  ({:test-run-id "7f399e7f-8979-411e-969f-e72c3471e48d",
  :similar
  (["17e003cb-d777-4646-90cc-8de001e94bb7" 92/95]
   ["b66dafe3-0358-4ff3-b46f-698cf4e8208d" 92/95]
   ["065d791d-3f00-4e71-8fcd-d956747b6d65" 91/95]
   ["c87e7562-0ea7-4289-94d2-c25264f2b23f" 91/95]
   ["822a65fb-bc8a-4b23-bc3e-e50571f608f1" 52/55])}
 {:test-run-id "39550af2-6ad3-46d5-84a0-4f4bc7838d7f",
  :similar
  (["17e003cb-d777-4646-90cc-8de001e94bb7" 93/94]
   ["b66dafe3-0358-4ff3-b46f-698cf4e8208d" 93/94]
   ["065d791d-3f00-4e71-8fcd-d956747b6d65" 46/47]
   ["c87e7562-0ea7-4289-94d2-c25264f2b23f" 46/47]
   ["c335bf8a-b0cd-417a-8bb9-c6442869cc8d" 95/99])}
 {:test-run-id "8b5c29b1-3b0f-4c82-8c76-61765d2e5589",
  :similar
  (["17e003cb-d777-4646-90cc-8de001e94bb7" 24/25]
   ["b66dafe3-0358-4ff3-b46f-698cf4e8208d" 24/25]
   ["065d791d-3f00-4e71-8fcd-d956747b6d65" 47/50]
   ["c87e7562-0ea7-4289-94d2-c25264f2b23f" 47/50]
   ["acdaf460-7683-460a-9919-315a37f00c5d" 73/81])})
  
  (println (find-query-plan-txt "7f399e7f-8979-411e-969f-e72c3471e48d"))

  (println (find-query-plan-txt "17e003cb-d777-4646-90cc-8de001e94bb7")) 
  
  )
  