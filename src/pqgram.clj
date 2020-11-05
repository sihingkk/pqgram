(ns pqgram
  (:require [clojure.zip :as z]
            [multiset.core :as ms]))

(defn do-n-times [loc n f]
  (nth (iterate f loc) n))

(defn add-leafs [loc n]
  (-> loc
      (z/edit vector)
      (do-n-times n #(z/insert-child % "*"))
      (do-n-times n z/next)))

(defn root? [loc]
  (= (z/node loc) (z/root loc)))

(defn surround-siblings [loc n]
  (-> loc
      (do-n-times n #(z/insert-left % "*"))
      (do-n-times n #(z/insert-right % "*"))))

(defn star? [loc]
  (= (z/node loc) "*"))

(defn add-parent-star [tree]
  ["*" tree])

(defn add-parent-stars [tree p]
  (do-n-times tree p add-parent-star))

(defn node->value [node]
  (if (vector? node) (first node) node))

(def loc->value (comp z/node node->value))

(defn full-path [loc]
  (conj (->> loc z/path (mapv node->value))
        (-> loc  z/node node->value)))

(defn take-consecutive-children [n loc]
  (partition n (->> loc z/children (map node->value))))

(defn prepend [path tails]
  (map #(concat path %) tails))

(defn take-n-parents [n loc]
  (->> loc full-path (take-last n) vec))

(defn extend-tree [zipper p q]
  (fn [tree]
    (loop [loc (zipper tree)]
      (cond
        (z/end? loc)
        (add-parent-stars (z/root loc) (dec p))

        (or (root? loc) (star? loc))
        (recur (z/next loc))

        (z/branch? loc)
        (recur (z/next (surround-siblings loc (dec q))))

        :else 
        (recur (z/next (add-leafs loc q)))))))

(defn pq-grams [zipper p q]
  (fn [tree]
    (loop [loc  (zipper ((extend-tree zipper p q) tree))
           result (ms/multiset)]
      (cond
        (z/end? loc)
        result

        (not (z/branch? loc))
        (recur (z/next loc) result)

        :else
        (let [subpaths (->> loc
                            (take-consecutive-children q)
                            (prepend (take-n-parents p loc))
                            (filter #(= (+ p q) (count %))))
              result'  (apply conj result subpaths)]
          (recur (z/next loc) result'))))))

(defn pq-gram-distance [zipper p q]
  (fn [x y]
    (let [pq-grams' (pq-grams zipper p q)
          xs (pq-grams' x)
          ys (pq-grams' y)
          intersection (count (ms/intersect xs ys))
          union (count (ms/sum xs ys))]
      (/ (- union (* 2 intersection))
         (- union intersection)))))