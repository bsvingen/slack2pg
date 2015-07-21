(ns com.borkdal.slack2pg.db
  (:refer-clojure :exclude [distinct distinct?])
  (:require [com.borkdal.slack2pg.config :as config]
            [clojure.java.jdbc :as jdbc]
            [com.borkdal.squirrel.postgresql :refer [select
                                                     distinct
                                                     insert
                                                     join
                                                     left-join
                                                     inner-join
                                                     table-name
                                                     column
                                                     column-alias
                                                     column-name
                                                     add
                                                     where
                                                     is-null
                                                     returning
                                                     compile-sql
                                                     natural
                                                     with-query
                                                     with-query-name
                                                     star]]
            [taoensso.timbre :as l]))

(def ^:private ^:const messages-table "messages")
(def ^:private ^:const teams-table "teams")
(def ^:private ^:const channels-table "channels")
(def ^:private ^:const users-table "users")
(def ^:private ^:const new-messages-table "new_messages")
(def ^:private ^:const new-teams-table "new_teams")
(def ^:private ^:const new-channels-table "new_channels")
(def ^:private ^:const new-users-table "new_users")
(def ^:private ^:const new-messages-teams-table "new_messages_teams")
(def ^:private ^:const new-messages-channels-table "new_messages_channels")
(def ^:private ^:const new-messages-users-table "new_messages_users")

(def ^:dynamic *db-con*)

(defn- make-db-spec
  []
  {:subprotocol "postgresql"
   :subname (str "//" (:db-host config/*config*)
                 ":" (:db-port config/*config*)
                 "/" (:db-name config/*config*))
   :user (:db-user config/*config*)
   :password (:db-password config/*config*)})

(defmacro with-transaction
  [& body]
  `(jdbc/with-db-transaction [db-con# (make-db-spec)]
     (binding [*db-con* db-con#]
       ~@body)))

(def ^:private ^:const column-definitions
  (str ))

(defn- create-tables
  [db-spec]
  {:post [(= % [0])]}
  (jdbc/execute! db-spec
                 [(str "create table if not exists "
                       teams-table
                       " ("
                       "team_id serial primary key, "
                       "slack_team_id text not null, "
                       "team_domain text not null"
                       ", unique ("
                       "team_id, "
                       "slack_team_id, "
                       "team_domain",
                       "))")])
  (jdbc/execute! db-spec
                 [(str "create table if not exists "
                       channels-table
                       " ("
                       "channel_id serial primary key, "
                       "slack_channel_id text not null, "
                       "channel_name text not null"
                       ", unique ("
                       "channel_id, "
                       "slack_channel_id, "
                       "channel_name",
                       "))")])
  (jdbc/execute! db-spec
                 [(str "create table if not exists "
                       users-table
                       " ("
                       "user_id serial primary key, "
                       "slack_user_id text not null, "
                       "user_name text not null"
                       ", unique ("
                       "user_id, "
                       "slack_user_id, "
                       "user_name",
                       "))")])
  (jdbc/execute! db-spec
                 [(str "create table if not exists "
                       messages-table
                       " ("
                       "id serial primary key, "
                       "team_id integer references "
                       teams-table
                       " on delete restrict not null, "
                       "channel_id integer references "
                       channels-table
                       " on delete restrict not null, "
                       "user_id integer references "
                       users-table
                       " on delete restrict not null, "
                       "timestamp timestamp with time zone not null, "
                       "text text not null"
                       ", unique ("
                       "team_id, "
                       "channel_id, "
                       "user_id, "
                       "timestamp, "
                       "text"
                       "))")]))

(defn- create-temp-table
  [db-spec]
  {:post [(= % [0])]}
  (jdbc/execute! db-spec
                 [(str "create temp table "
                       new-messages-table
                       " ("
                       "slack_team_id text not null , "
                       "team_domain text not null, ",
                       "slack_channel_id text not null, ",
                       "channel_name text not null, ",
                       "slack_user_id text not null, ",
                       "user_name text not null, ",
                       "timestamp timestamp with time zone not null, "
                       "text text not null"
                       ")")]))

(defn- drop-tables
  [db-spec]
  {:post [(= % [0])]}
  (jdbc/execute! db-spec [(str "drop table if exists " messages-table)])
  (jdbc/execute! db-spec [(str "drop table if exists " teams-table)])
  (jdbc/execute! db-spec [(str "drop table if exists " channels-table)])
  (jdbc/execute! db-spec [(str "drop table if exists " users-table)]))

(defn- valid-entry?
  [{:keys [slack_team_id
           team_domain
           slack_channel_id
           channel_name
           slack_user_id
           user_name
           timestamp
           text]}]
  (and slack_team_id
       team_domain
       slack_channel_id
       channel_name
       slack_user_id
       user_name
       timestamp
       text))

(defn- init-db
  []
  (create-tables *db-con*)
  (create-temp-table *db-con*))

(defn- make-coalesce
  [id-field
   joined-messages-table
   new-table]
  (str "coalesce("
       joined-messages-table
       "."
       id-field
       ", "
       new-table
       "."
       id-field
       ")"))

(defn- join-new-messages-with-reference-table
  [query-name
   referenced-table-name]
  (with-query
    (with-query-name query-name)
    (select
     (star)
     (join
      (natural)
      (left-join)
      (table-name new-messages-table)
      (table-name referenced-table-name)))))

(defn- insert-missing-references
  [query-name
   referenced-table-name
   joined-messages-table
   id-field
   slack-id-field
   slack-name-field]
  (let [slack-columns (map column [slack-id-field
                                   slack-name-field])]
    (with-query
      (with-query-name query-name)
      (add
       (insert
        (table-name referenced-table-name)
        (add
         (select
          (distinct)
          (table-name joined-messages-table)
          (where (is-null id-field)))
         slack-columns)
        (add
         (returning
          (column id-field))
         slack-columns))
       slack-columns))))

(defn- combine-old-and-new-references
  [query-name
   joined-messages-table
   inserted-references-table
   id-field
   slack-id-field
   slack-name-field]
  (with-query
    (with-query-name query-name)
    (add
     (select
      (column (make-coalesce id-field
                             joined-messages-table
                             inserted-references-table)
              (column-alias id-field))
      (join
       (left-join)
       (table-name joined-messages-table)
       (table-name inserted-references-table)
       (column-name slack-id-field)
       (column-name slack-name-field)))
     (map column ["slack_team_id"
                  "team_domain"
                  "slack_channel_id"
                  "channel_name"
                  "slack_user_id"
                  "user_name"
                  "timestamp"
                  "text"]))))

(defn- get-join-new-messages-with-references-queries
  []
  [(join-new-messages-with-reference-table new-messages-teams-table
                                           teams-table)
   (join-new-messages-with-reference-table new-messages-channels-table
                                           channels-table)
   (join-new-messages-with-reference-table new-messages-users-table
                                           users-table)])

(defn- get-insert-missing-references-queries
  []
  [(insert-missing-references new-teams-table
                              teams-table
                              new-messages-teams-table
                              "team_id"
                              "slack_team_id"
                              "team_domain")
   (insert-missing-references new-channels-table
                              channels-table
                              new-messages-channels-table
                              "channel_id"
                              "slack_channel_id"
                              "channel_name")
   (insert-missing-references new-users-table
                              users-table
                              new-messages-users-table
                              "user_id"
                              "slack_user_id"
                              "user_name")])

(defn- get-combine-old-and-new-references-queries
  []
  [(combine-old-and-new-references "combined_teams"
                                   new-messages-teams-table
                                   new-teams-table
                                   "team_id"
                                   "slack_team_id"
                                   "team_domain")
   (combine-old-and-new-references "combined_channels"
                                   new-messages-channels-table
                                   new-channels-table
                                   "channel_id"
                                   "slack_channel_id"
                                   "channel_name")
   (combine-old-and-new-references "combined_users"
                                   new-messages-users-table
                                   new-users-table
                                   "user_id"
                                   "slack_user_id"
                                   "user_name")])

(defn- make-insert-query
  []
  (let [columns (map column ["team_id"
                             "channel_id"
                             "user_id"
                             "timestamp"
                             "text"])]
    (add
     (insert
      (table-name messages-table)
      (add
       (select
        (distinct)
        (join
         (natural)
         (left-join)
         (join
          (natural)
          (inner-join)
          (table-name "combined_teams")
          (table-name "combined_channels")
          (table-name "combined_users"))
         (table-name messages-table))
        (where (is-null "id")))
       columns)
      (returning
       (column "id")))
     (concat (get-join-new-messages-with-references-queries)
             (get-insert-missing-references-queries)
             (get-combine-old-and-new-references-queries)
             columns))))

(defn- write-to-temp-table
  [entries]
  {:pre [(sequential? entries)
         #_(every? valid-entry? entries)]
   :post [(sequential? %)
          (= (count %) (count entries))]}
  (let [result (apply jdbc/insert! *db-con* new-messages-table entries)]
    (l/info (format "%d messages read." (count result)))
    result))

(defn- select-into-table
  []
  (let [result (jdbc/query *db-con* [(compile-sql (make-insert-query))])]
    (l/info (format "%d messages inserted." (count result)))
    result))

(defn write-batch-to-db
  [batch]
  (with-transaction
    (init-db)
    (write-to-temp-table batch)
    (select-into-table)))

