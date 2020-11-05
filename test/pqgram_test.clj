(ns pqgram-test
  (:require [pqgram :refer :all]
            [clojure.test :refer :all]            
            [multiset.core :as ms]))

(def tree-A ["a" ["b"  "c"] ["b"  "d"  "c"]  "e"])
(def tree-B ["a" ["b" "e" "d"]])

(deftest generating-multisets
  (is (= 
      (ms/multiset ["*" "a" "b"] ["*" "a" "b"] ["*" "a" "e"] ["a" "b" "c"] ["a" "b" "d"]
                   ["a" "b" "c"] ["a" "e" "*"] ["b" "c" "*"] ["b" "d" "*"] ["b" "c" "*"])
       (pq-gram-multisets tree-A 2 1)))
 (is (=
      (ms/multiset  ["*" "a" "b"] ["a" "b" "e"] ["a" "b" "d"] ["b" "e" "*"] ["b" "d" "*"])
      (pq-gram-multisets tree-B 2 1))))

(deftest dinstance-test 
  (is (= 75/100 (dinstance tree-A tree-B 2 1))))