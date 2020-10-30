(ns pqgram
  (:require [clojure.zip :as z]))

(defn do-n-times [loc n f]
  (nth (iterate f loc) n))

(defn add-leafs [loc n]
  (-> loc
      (z/edit vector)
      (do-n-times n #(z/insert-child % "*"))
      (do-n-times n z/next)))

(defn root? [loc]
  (= (z/node loc) (z/root loc)))

(defn add-siblings [loc n]
  (-> loc
      (do-n-times n #(z/insert-left % "*"))
      (do-n-times n #(z/insert-right % "*"))))

(defn star? [loc]
  (= (z/node loc) "*"))

(defn pqgram [original p q]
  (loop [loc  original]
    (cond
      (z/end? loc)
      (z/root loc)

      (or (root? loc) (star? loc))
      (recur (z/next loc))

      (z/branch? loc)
      (recur (z/next (add-siblings loc (- p 1))))

      :else (recur (z/next (add-leafs loc q))))))

(defn btree-zipper [btree]
  (z/zipper vector?
            rest
            (fn [[x _ _] children]
              (vec (cons x children)))
            btree))

(let [btree ["a" ["b"  "c"] ["b"  "d"  "c"]  "e"]
      root-loc (btree-zipper btree)
      p 2
      q 1]
  (pqgram root-loc p q))












