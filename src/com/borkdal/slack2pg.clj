(ns com.borkdal.slack2pg
  (:require [com.borkdal.slack2pg.config :as config]
            [com.borkdal.slack2pg.sqs :as sqs]
            [com.borkdal.slack2pg.export :as export]
            [com.borkdal.slack2pg.cloudformation :as cloudformation]
            [clojure.java.io :as io])
  (:gen-class))

(defn- error
  []
  (println "See README.md for instructions on how to run.")
  (System/exit 1))

(defn -main
  ([& args]
   (let [[config-edn-filename
          command
          slack-export-pathname] args]
     (when-not (and config-edn-filename
                    command)
       (error))
     (config/with-config-file [config-edn-filename]
       (case command
         "sqs" (loop []
                 (try
                   (sqs/write-all-sqs-messages-to-database)
                   (catch Throwable e
                     (println "Caught" e)
                     (println "Retrying")))
                 (recur))
         "aws-create" (cloudformation/deploy)
         "read-export" (export/write-export-messages-to-database slack-export-pathname)
         (error))))))

