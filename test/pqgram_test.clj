(ns pqgram-test
  (:require [pqgram :refer :all]
            [clojure.zip :as z]
            [clojure.test :refer :all]            
            [multiset.core :as ms]))

(defn tree-zipper [tree]
  (z/zipper vector?
            rest
            (fn [[x _ _] children]
              (vec (cons x children)))
            tree))

(def tree-A ["a" ["b"  "c"] ["b"  "d"  "c"]  "e"])
(def tree-B ["a" ["b" "e" "d"]])

(deftest generating-pq-grams
  (is (=
       (ms/multiset ["*" "a" "b"] ["*" "a" "b"] ["*" "a" "e"] ["a" "b" "c"] ["a" "b" "d"]
                    ["a" "b" "c"] ["a" "e" "*"] ["b" "c" "*"] ["b" "d" "*"] ["b" "c" "*"])
       ((pq-grams tree-zipper 2 1) tree-A)))
  (is (=
       (ms/multiset  ["*" "a" "b"] ["a" "b" "e"] ["a" "b" "d"] ["b" "e" "*"] ["b" "d" "*"])
       ((pq-grams tree-zipper 2 1) tree-B))))

(deftest distance-test 
  (is (= 75/100 ((pq-gram-distance tree-zipper 2 1) tree-A tree-B))))

