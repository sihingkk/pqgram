(ns db
  (:require [neo4j-clj.core :as db])
  (:import (java.net URI)))

(def local-db
  (db/connect (URI. "")
              ""
              ""))