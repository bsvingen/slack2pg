(defproject com.borkdal/slack2pg "0.1.0-SNAPSHOT"
  :description "A tool for storing Slack messages in PostgreSQL"
  :url "https://github.com/bsvingen/slack2pg"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.postgresql/postgresql "9.4-1201-jdbc41"]
                 [org.clojure/java.jdbc "0.3.7"]
                 [amazonica "0.3.29"]
                 [com.comoyo/condensation "0.2.2"]
                 [org.tobereplaced/lettercase "1.0.0"]
                 [com.borkdal/clojure.utils "0.1.2"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [clj-time "0.10.0"]
                 [com.borkdal/squirrel "0.3.1"]
                 [com.taoensso/timbre "4.0.2"]]
  :main com.borkdal.slack2pg
  :target-path "target/%s"
  :plugins [[lein-bin "0.3.5"]]
  :bin {:name "slack2pg"
        :bin-path "target"}
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[midje "1.7.0"]]
                   :plugins [[lein-midje "3.1.3"]]}})
