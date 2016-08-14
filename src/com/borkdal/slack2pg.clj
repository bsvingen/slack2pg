(ns com.borkdal.slack2pg
  (:require [com.borkdal.slack2pg.config :as config]
            [com.borkdal.slack2pg.sqs :as sqs]
            [com.borkdal.slack2pg.export :as export]
            [com.borkdal.slack2pg.cloudformation :as cloudformation]
            [clojure.java.io :as io]
            [clojure.spec :as s])
  (:gen-class))

(defn is?
  [s]
  (partial = s))

(s/def ::args (s/cat :config-edn-filename string?
                     :commands (s/alt :sqs (is? "sqs")
                                      :aws-create (is? "aws-create")
                                      :read-export (s/cat :command (is? "read-export")
                                                          :slack-export-pathname string?))))
(defn- error
  []
  (println "See README.md for instructions on how to run.")
  (System/exit 1))

(defn -main
  ([& args]
   (let [parsed-args (s/conform ::args args)]
     (when (= parsed-args :clojure.spec/invalid)
       (error))
     (let [{:keys [config-edn-filename commands]} parsed-args
           [command options] commands]
       (config/with-config-file [config-edn-filename]
         (case command
           :sqs (loop []
                  (try
                    (sqs/write-all-sqs-messages-to-database)
                    (catch Throwable e
                      (println "Caught" e)
                      (println "Retrying")))
                  (recur))
           :aws-create (cloudformation/deploy)
           :read-export (export/write-export-messages-to-database
                         (:slack-export-pathname options))
           (error)))))))

