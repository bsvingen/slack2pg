(ns com.borkdal.slack2pg.sqs
  (:require [com.borkdal.slack2pg.cloudformation :as cloudformation]
            [amazonica.aws.sqs :as sqs]
            [clojure.data.json :as json]
            [com.borkdal.slack2pg.config :as config]
            [com.borkdal.slack2pg.common :as common]
            [com.borkdal.slack2pg.db :as db]
            [clojure.core.async :as a]
            [taoensso.timbre :as l]))

(defn body-map
  [f]
  (map #(if (= :flush %)
          %
          (update % :body f))))

(defn- parse-messages-for-db-xf
  [& {:keys [batch-size]
      :or {batch-size 100}}]
  (comp (body-map json/read-str)
        (body-map common/keywordize-keys)
        (body-map common/parse-timestamp-value)
        (body-map common/select-keys-for-database)
        (body-map common/rename-id-fields)
        (common/flushable-partition-all batch-size)))

(defn- get-messages
  [queue-url]
  (:messages
   (sqs/receive-message :queue-url queue-url
                        :wait-time-seconds 10
                        :max-number-of-messages 10)))

(defn- keep-reading-from-sqs-queue
  [queue-url
   channel]
  (let [get-more (partial get-messages queue-url)]
    (a/go
      (try
        (loop [messages (get-more)]
          (cond
            (not (seq messages)) (recur (get-more))
            (a/>! channel (first messages)) (recur (rest messages))))
        (catch Throwable e
          (l/fatal e)
          e)))))

(defn- flush-transducer-every-ten-seconds
  [c]
  (loop []
    (Thread/sleep 10000)
    (a/>!! c :flush)
    (recur)))

(defn- delete-message-for-queue
  [queue-url
   message]
  (sqs/delete-message (assoc message :queue-url queue-url)))

(defn- keep-writing-sqs-messages-to-database
  [delete-message-f
   message-channel]
  (let [flush-future (future (flush-transducer-every-ten-seconds message-channel))]
    (a/go
      (try
        (loop []
          (let [batch (a/<! message-channel)]
            (when batch
              (when (instance? Throwable batch)
                (throw batch))
              (db/write-batch-to-db (map :body batch))
              (doseq [message batch]
                (delete-message-f message))
              (recur))))
        (catch Throwable e
          (l/fatal e)
          e)
        (finally
          (future-cancel flush-future))))))

(defn- exception-handler
  [e]
  (l/fatal e)
  e)

(defn write-all-sqs-messages-to-database
  [&
   {:keys [signal-finish-channel]
    :or {signal-finish-channel (a/chan)}}]
  (let [team-domain (:team config/*config*)
        queue-url (cloudformation/get-queue-url-for-team team-domain)
        delete-message-f (partial delete-message-for-queue queue-url)
        message-channel (a/chan 100 (parse-messages-for-db-xf) exception-handler)
        reading-result-channel (keep-reading-from-sqs-queue queue-url
                                                            message-channel)
        writing-result-channel (keep-writing-sqs-messages-to-database delete-message-f
                                                                      message-channel)]
    (try
      (loop []
        (let [[result channel] (a/alts!! [reading-result-channel
                                          writing-result-channel
                                          signal-finish-channel])]
          (when (instance? Throwable result)
            (throw result))
          (when (not (= channel signal-finish-channel))
            (recur))))
      (finally
        (a/close! message-channel)))))

