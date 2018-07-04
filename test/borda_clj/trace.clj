(ns borda-clj.trace
  (:require  [clojure.test :as t]
             [borda-clj.client :as sut]
             [clojure.core.async :as async :refer (<! <!! >! >!! alts! chan close! go go-loop onto-chan timeout)]))

(defn run-trace [global-dimensions max-buffer-size trace]
  (let [[report-ch measurements-ch kill-ch timeout-ch trace-ch send-ch] (repeatedly chan)
        successful-send (fn [m] (>!! trace-ch {:send m}))
        failed-send (fn [& args] (throw (RuntimeException. "boom")))
        final-send (fn [m] (>!! trace-ch {:final-send m}))
        send (fn [measurements] ((or (:trace-metadata (meta measurements))
                                    final-send)
                                measurements))
        on-send-error (fn [m e]
                        (clojure.pprint/pprint e)
                        (>!! trace-ch {:error {:measurements m}}))
        flush-ch (sut/go-flush-measurements report-ch measurements-ch kill-ch global-dimensions ::bogus-interval send on-send-error (constantly timeout-ch))]
    (sut/go-reduce-reports report-ch measurements-ch kill-ch max-buffer-size)
    (go
      (<! flush-ch)
      (close! trace-ch))
    (go
      (doseq [op trace]
        (case op
          :successful-tick (>! timeout-ch successful-send)
          :failed-tick (>! timeout-ch failed-send)
          :delay (<! (timeout 100))
          (>! report-ch op)))
      (<! (timeout 100))
      (close! report-ch))
    (<!! (async/into [] trace-ch))))

(comment

  (clojure.pprint/pprint
   (run-trace {:g "global"}
              2
              [[{:d "d"} {:v 1}]
               [{:e "e"} {:v 1}]
               [{:f "f"} {:v 1}]]))

  )
