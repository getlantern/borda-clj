(ns borda-clj.client-test
  (:require [clojure.test :refer :all]
            [borda-clj.client :refer :all]))

(deftest test-collect
  (let [da      (hash-map "x" "x" "y" "y")
        db      (hash-map "x" "xx" "y" "yy")
        va      (hash-map "i" 1 "ii" 10)
        vb      (hash-map "i" 2 "ii" 20)
        b1      (hash-map)
        b2      (hash-map da va)
        bmerged (hash-map da (hash-map "i" 3 "ii" 30))]
    (testing "Buffer equals self"
      (is (= b2 b2)))
    (testing "Collecting to empty"
      (is (= b2 (collect b1 1 da va))))
    (testing "Collecting to existing"
      (is (= bmerged (collect b2 1 da vb))))
    (testing "Collecting to full"
      (is (= b2 (collect b2 1 db vb))))))

(deftest test-reducing-submitter
  (let [da            (hash-map "x" "x" "y" "y")
        va            (hash-map "i" 1 "ii" 10)
        vb            (hash-map "i" 2 "ii" 20)
        bmerged       (hash-map da (hash-map "i" 3 "ii" 30))
        fail          (atom true)
        result        (atom {})
        update        (fn [next]
                        (if @fail
                          (do (reset! fail false) (throw (Exception. "I'm failing"))) ; fail on first send
                          (swap! result (fn [orig] (if (= orig {}) next orig)))))     ; update on subsequent send
        log-error     (fn [measurements e] (println "error on sending" (count measurements) "measurements to borda"))
        [submit stop] (reducing-submitter 10 1 update log-error)]
    (testing "Submit works"
      (submit da va)
      (submit da vb)
      (Thread/sleep 3000)
      (stop)
      (is (= bmerged @result)))))