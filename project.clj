(defproject borda-clj "0.0.1"
  :description "Clojure library for interacting with Borda"
  :url "https://github.com/getlantern/borda-clj"
  :license {:name "Apache Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [aleph "0.4.4"]
                 [io.netty/netty-tcnative-boringssl-static "2.0.0.Final"]
                 [io.netty/netty-all "4.1.11.Final"]
                 [cheshire "5.8.0"]
                 [slingshot "0.12.2"]]
  :min-lein-version "2.6.1"
  :source-paths ["src"]
  ;;:java-source-paths ["java"]

  :clean-targets [:target-path "target"])
