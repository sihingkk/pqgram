(ns pqgram-test
  (:require [pqgram :refer :all]
            [clojure.test :refer :all]
             [clojure.spec.alpha :as s]))

(deftest adding-numbers
  (is (= 4 (plus 2 2))))

(deftest dividing-numbers
  (is (= 2 (divide 4 2))))

(deftest dividing-numbers-by-zero
  (is (thrown? ArithmeticException (divide 1 0))))


(defn simple-fn []
  "x")

(s/fdef simple-fn :ret :simple/int)

(deftest spec-fail-test
  (is (= "x" (simple-fn)) "Just testing simple-fn"))
