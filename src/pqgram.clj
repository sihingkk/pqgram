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

(defn add-siblings [loc n]
  (-> loc
      (do-n-times n #(z/insert-left % "*"))
      (do-n-times n #(z/insert-right % "*"))))

(defn star? [loc]
  (= (z/node loc) "*"))

(defn add-parent-star [tree]
  ["*" tree])

(defn add-parent-stars [tree p]
  (do-n-times tree p add-parent-star))

(defn pqgram [original p q]
  (loop [loc  original]
    (cond
      (z/end? loc)
      (add-parent-stars (z/root loc) (dec p))

      (or (root? loc) (star? loc))
      (recur (z/next loc))

      (z/branch? loc)
      (recur (z/next (add-siblings loc (dec q))))

      :else (recur (z/next (add-leafs loc q))))))

(defn btree-zipper [btree]
  (z/zipper vector?
            rest
            (fn [[x _ _] children]
              (vec (cons x children)))
            btree))

(defn node->value [node] 
  (if (vector? node) (first node) node))

(def loc->value (comp z/node node->value))

(defn full-path [loc]
  (conj (->> loc z/path (mapv node->value))
        (-> loc  z/node node->value)))

(defn take-n-children [n loc] 
  (->> loc z/children (map node->value) (take n)))

(defn prepend [path tails]
  (map #(conj path %) tails))

(defn take-n-parents [n loc]
  (->> loc full-path (take-last n) vec))


(defn pq-gram-multisets 
  ([tree p q] 
   (pq-gram-multisets 
    (btree-zipper (pqgram (btree-zipper tree) p q)) (ms/multiset) p q))
  
  ([loc result p q] 
   (cond
     (z/end? loc)
     result

     (not (z/branch? loc))
     (recur (z/next loc) result p q)

     :else (let [subpaths    (->> loc
                                  (take-n-children 100) ;; FIXME
                                  (prepend (take-n-parents p loc))
                                  (filter #(= (+ p 1) (count %))))
                 result'  (apply conj result subpaths)]
             (recur (z/next loc) result' p q)))))

(defn dinstance [x y p q]
  (let [xs (pq-gram-multisets x p q)
        ys (pq-gram-multisets y p q)
        intersection (count (ms/intersect xs ys))
        union (count (ms/sum xs ys))]
    (/ (- union (* 2 intersection))
       (- union intersection))))
    

