(ns borda-clj.client-test
  (:require [clojure.test :refer :all]
            [borda-clj.client :refer :all]))

(deftest test-collect
  (let [da      {:x "x" :y "y"}
        db      {:x "xx" :y "yy"}
        va      {:i 1 :ii 10}
        vb      {:i 2 :ii 20}
        b1      {}
        b2      {da va}
        bmerged {da {:i 3 :ii 30 :_submits 1}}]
    (testing "Buffer equals self"
      (is (= b2 b2)))
    (testing "Collecting to empty"
      (is (= {da (assoc va :_submits 1)} (collect b1 1 da va))))
    (testing "Collecting to existing"
      (is (= bmerged (collect b2 1 da vb))))
    (testing "Collecting to full"
      (is (= (submit-failed b2 {:_submits 1}) (collect b2 1 db vb))))))

(deftest test-reducing-submitter
  (let [global  {:gk "global"}
        da      {:x "x" :y "y"}
        db      {:x "xx" :y "yy"}
        va      {:i 1 :ii 10}
        vb      {:i 2 :ii 20}
        bmerged {(merge global da) {:i 3 :ii 30} (merge global {:op "_submit"}) {:success_count 2 :error_count 1}}
        fail          (atom true)
        result        (atom {})
        update        (fn [next]
                        (if @fail
                          (do (reset! fail false) (throw (Exception. "I'm failing"))) ; fail on first send
                          (swap! result (fn [orig] (if (= orig {}) next orig)))))     ; update on subsequent send
        log-error     (fn [measurements e] (println "error on sending" (count measurements) "measurements to borda" e))
        [submit stop] (reducing-submitter global 1 100 update log-error)]
    (testing "Submit works"
      (submit da va)
      (submit da vb)
      (submit db va) ; this should be discarded because buffer is full
      (Thread/sleep 300)
      (stop)
      (is (= bmerged @result)))))
