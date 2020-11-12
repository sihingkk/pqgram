(ns pqgram
  (:require [clojure.zip :as z]
            [multiset.core :as ms]))

(defn do-n-times [loc n f]
  (nth (iterate f loc) n))

(defn add-leafs [loc n]
  (-> loc
      (z/edit vector) ;; TODO: make that injectable
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

(defn add-parent-star [tree] ;; TODO: make that injectable
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

(defn prepend [v v']
  (map #(concat v %) v'))

(defn take-n-parents-child-path [n loc]
  (->> loc full-path (take-last n) vec))

(defn count= [n]
  #(= n (count %)))

(def leaf? (complement z/branch?))

(defn extend-tree [zipper p q tree]
  (loop [loc (zipper tree)]
    (if (z/end? loc)
      (zipper (add-parent-stars (z/root loc) (dec p)))
      (recur (z/next (cond
                       (or (root? loc) (star? loc))
                       loc

                       (z/branch? loc)
                       (surround-siblings loc (dec q))

                       :else
                       (add-leafs loc q)))))))

(defn loc->pq-grams [loc p q]
  (if (leaf? loc)
    []
    (filter (count= (+ p q))
            (prepend
             (take-n-parents-child-path p loc)
             (take-consecutive-children q loc)))))

(defn pq-grams [zipper p q]
  (fn [tree]
    (loop [loc (extend-tree zipper p q tree)
           acc (ms/multiset)]
     (if (z/end? loc)
       acc
       (recur (z/next loc) 
              (apply conj acc (loc->pq-grams loc p q)))))))

(defn pq-gram-distance [zipper p q]
  (fn [xs ys]
    (let [pq-grams' (pq-grams zipper p q)
          xs' (pq-grams' xs)
          ys' (pq-grams' ys)
          intersection (count (ms/intersect xs' ys'))
          union (count (ms/sum xs' ys'))]
      (/ (- union (* 2 intersection))
         (- union intersection)))))
