(ns plans
  (:require [pqgram :refer [pq-gram-distance]]
            [neo4j-clj.core :as db]
            [clojure.zip :as z])
  (:import (java.net URI)))

(def local-db
  (db/connect (URI. "")
              ""
              ""))

(db/defquery root-operator
  "MATCH (testRun:TestRun {id:$id})-[:HAS_METRICS]->(metrics:Metrics)-[:HAS_PLAN]->(plan:Plan),
   (plan)-[:HAS_PLAN_TREE]->(tree:PlanTree)-[:HAS_OPERATORS]->(operator:Operator),
   p = (operator:Operator)
   RETURN operator.operator_type as name, id (operator) as id")

(defn root [id]
  (with-open [session (db/get-session local-db)]
    (first (root-operator session {:id id}))))


(db/defquery operator-children
  "MATCH (operator:Operator)-[:HAS_CHILD]->(children:Operator)
   WHERE id (operator) = $id   
   RETURN children.operator_type as name, id(children) as id")

(defn children-by-id [id]
  (with-open [session (db/get-session local-db)]
    (vec (doall  (operator-children session {:id id})))))


(db/defquery test-run-ids
  "MATCH (m:Metrics)-[:METRICS_FOR]->(b:Benchmark )<-[:HAS_BENCHMARK]-(bg:BenchmarkGroup {name: $name}),
         (m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)-[:WITH_PROJECT]->(p:Project {commit: $commit})
   RETURN tr.id as id")

(defn find-test-run-ids [{:keys [commit group-name]}]
  (with-open [session (db/get-session local-db)]
    (doall  (mapv :id (test-run-ids session {:commit commit :name group-name})))))

(defn tree

  ([operator]
   (tree operator []))

  ([{:keys [name id]} acc]
   (let [children (children-by-id  id)]
     (if (empty? children)
       name
       (vec (concat acc [name] (mapv #(tree % []) children)))))))

(defn tree-zipper [tree]
  (z/zipper vector?
            rest
            (fn [[x _ _] children]
              (vec (cons x children)))
            tree))

(def d (pq-gram-distance tree-zipper 3 2))

(defn ->tree [test-run-id]
  (tree (root test-run-id)))


(let [tr-ids (find-test-run-ids {:commit "0b7d6e27a7cd0534b1a9f27607b1b99ab6444774"
                                 :group-name "accesscontrol"})
      example (->tree (last tr-ids))
      to-compare (map ->tree (take 5 tr-ids))]
  (->> to-compare 
     (map (fn [t] (d example t)))))

