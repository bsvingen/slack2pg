(ns com.borkdal.slack2pg.config
  (:require [clojure.edn :as edn]))

(def ^:dynamic *config* nil)

(defn- config-ok?
  [{:keys [db-host
           db-port
           db-name
           db-user
           db-password
           team]}]
  (and db-host
       db-port
       db-name
       db-user
       db-password
       team))

(defn read-config-edn
  [edn-filename]
  {:post [(config-ok? %)]}
  (edn/read-string
   (slurp edn-filename)))

; Only for REPL use
(defn set-config!
  [config]
  {:pre [(config-ok? config)]}
  (alter-var-root #'*config* (constantly config)))

; Only for REPL use
(defn set-config-from-file!
  [edn-filename]
  (set-config! (read-config-edn edn-filename)))

(defmacro with-config
  [[config] & body]
  `(binding [*config* ~config]
     ~@body))

(defmacro with-config-file
  [[config-edn-filename] & body]
  `(with-config [(read-config-edn ~config-edn-filename)]
     ~@body))

