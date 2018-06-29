(def project 'borda-clj)
(def version "0.1.0-SNAPSHOT")

(set-env! :resource-paths #{"resources" "src"}
          :source-paths   #{"test"}
          :dependencies   '[[org.clojure/clojure "1.9.0"]
                            [adzerk/boot-test "RELEASE" :scope "test"]
                            [aleph "0.4.4"]
                            [cheshire "5.8.0"]
                            [slingshot "0.12.2"]])

(task-options!
 pom {:project     project
      :version     version
      :description "Clojure library for interacting with Borda"
      :url         "https://github.com/getlantern/borda-clj"
      :scm         {:url "https://github.com/getlantern/borda-clj"}
      :license     {"Apache License 2.0"
                    "https://www.apache.org/licenses/LICENSE-2.0"}})

(deftask build
  "Build and install the project locally."
  []
  (comp (pom) (jar) (install)))

(require '[adzerk.boot-test :refer [test]])
