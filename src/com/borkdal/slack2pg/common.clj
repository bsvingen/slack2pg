(ns com.borkdal.slack2pg.common
  (:require [org.tobereplaced.lettercase :as lc]
            [clojure.set :as set]
            [clj-time.coerce :as ctime]))

(defn- keywordize-key
  [[key value]]
  {(if (keyword? key)
     key
     (lc/lower-underscore-keyword key))
   value})

(defn keywordize-keys
  [input-map]
  (into {}
        (map keywordize-key input-map)))

(def keywordize-keys-xf
  (map keywordize-keys))

(defn- parse-timestamp
  [unix-time-string]
  (-> unix-time-string
      (Double.)
      (* 1000)
      (Math/round)
      (Long.)
      (ctime/to-sql-time)))

(defn parse-timestamp-value
  [entry-map]
  (assoc entry-map
         :timestamp
         (parse-timestamp
          (:timestamp entry-map))))

(def parse-timestamps-xf
  (map parse-timestamp-value))

(defn select-keys-for-database
  [entry-map]
  (select-keys entry-map [:team_id
                          :team_domain
                          :channel_id
                          :channel_name
                          :user_id
                          :user_name
                          :timestamp
                          :text]))

(def select-keys-for-database-xf
  (map select-keys-for-database))

(defn rename-id-fields
  [entry-map]
  (set/rename-keys entry-map {:team_id :slack_team_id
                              :channel_id :slack_channel_id
                              :user_id :slack_user_id}))

(def rename-id-fields-xf
  (map rename-id-fields))

(defn flushable-partition-all
  [^long n]
  (fn [rf]
    (let [a (java.util.ArrayList. n)]
      (letfn [(flush
                [result]
                (let [result (if (.isEmpty a)
                               result
                               (let [v (vec (.toArray a))]
                                 (.clear a)
                                 (unreduced (rf result v))))]
                  (rf result)))]
        (fn r
          ([] (rf))
          ([result]
           (flush result))
          ([result input]
           (if (= input :flush)
             (flush result)
             (do
               (.add a input)
               (if (= n (.size a))
                 (let [v (vec (.toArray a))]
                   (.clear a)
                   (rf result v))
                 result)))))))))

