(ns com.borkdal.slack2pg.export
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [com.borkdal.clojure.utils :as utils]
            [com.borkdal.slack2pg.config :as config]
            [com.borkdal.slack2pg.common :as common]
            [com.borkdal.slack2pg.db :as db])
  (:import [java.util.zip ZipFile]))

(defn- get-zip-json
  [filename
   zipfile]
  (map common/keywordize-keys
       (json/read
        (io/reader
         (.getInputStream zipfile (.getEntry zipfile filename))))))

(def get-zip-users
  (partial get-zip-json "users.json"))

(def get-zip-channels
  (partial get-zip-json "channels.json"))

(defn- make-users-id->name-map
  [users]
  (into {}
        (map (juxt :id :name)
             users)))

(defn- make-channels-name->id-map
  [channels]
  (into {}
        (map (juxt :name :id)
             channels)))

(def ignore-directories-xf
  (filter #(not (.isDirectory %))))

(defn- extract-channel-name
  [entry]
  {:entry entry
   :channel-name (second
                  (utils/only
                   (re-seq #"^(\w+)/\d\d\d\d-\d\d-\d\d.json$"
                           (.getName entry))))})

(def keep-only-channel-files-xf
  (comp (map extract-channel-name)
        (filter :channel-name)))

(defn- zipfile-json->map
  [zipfile
   zip-entry]
  (assoc zip-entry
         :message-maps
         (json/read
          (io/reader
           (.getInputStream zipfile (:entry zip-entry))))))

(defn- make-parse-channel-files-xf
  [zipfile]
  (map (partial zipfile-json->map zipfile)))

(defn add-extra-info
  [team-id
   team-domain
   channel-name
   channel-id
   users-id->name-map
   message-maps]
  (map (fn [message-map]
         (assoc message-map
                :team_id team-id
                :team_domain team-domain
                :user_name (users-id->name-map (message-map "user"))
                :channel_id channel-id
                :channel_name channel-name))
       message-maps))

(defn- add-extra-info-to-message-maps
  [team-id
   team-domain
   channels-name->id-map
   users-id->name-map
   file-maps]
  (let [channel-name (:channel-name file-maps)
        channel-id (channels-name->id-map channel-name)]
    (update file-maps
            :message-maps
            (partial add-extra-info
                     team-id
                     team-domain
                     channel-name
                     channel-id
                     users-id->name-map))))

(defn- make-add-extra-info-to-message-maps-xf
  [team-id
   team-domain
   channels-name->id-map
   users-id->name-map]
  (map
   (partial add-extra-info-to-message-maps
            team-id
            team-domain
            channels-name->id-map
            users-id->name-map)))

(def extract-messages-from-message-maps
  (mapcat :message-maps))

(def rename-fields-xf
  (map
   (fn [message-map]
     (set/rename-keys message-map
                      {:ts :timestamp :user :user_id}))))

(def keep-only-messages-xf
  (filter
   #(= "message" (:type %))))

(def remove-comments-xf
  (filter (complement :comment)))

(defn- make-zip-parser-xf
  [zipfile
   team-id
   team-domain]
  (let [channels-name->id-map (make-channels-name->id-map
                               (get-zip-channels zipfile))
        users-id->name-map (make-users-id->name-map
                            (get-zip-users zipfile))]
    (comp ignore-directories-xf
          keep-only-channel-files-xf
          (make-parse-channel-files-xf zipfile)
          (make-add-extra-info-to-message-maps-xf team-id
                                                  team-domain
                                                  channels-name->id-map
                                                  users-id->name-map)
          extract-messages-from-message-maps
          common/keywordize-keys-xf
          rename-fields-xf
          common/parse-timestamps-xf
          keep-only-messages-xf
          remove-comments-xf
          common/select-keys-for-database-xf
          common/rename-id-fields-xf
          (common/flushable-partition-all 1000))))

(defn- get-zip-messages
  [slack-export-pathname
   team-id
   team-domain]
  (let [zipfile (ZipFile. slack-export-pathname)]
    (transduce (make-zip-parser-xf zipfile team-id team-domain)
               conj
               (enumeration-seq (.entries zipfile)))))

(defn write-export-messages-to-database
  [slack-export-pathname]
  (doseq [batch (get-zip-messages slack-export-pathname
                                  (:team-id config/*config*)
                                  (:team config/*config*))]
    (db/write-batch-to-db batch)))

