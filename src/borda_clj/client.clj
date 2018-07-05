(ns borda-clj.client
  (:use [slingshot.slingshot :only [throw+ try+]])
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure.core.async :as async :refer (<! >!! <!! alts! chan close! go go-loop onto-chan timeout)]))

(def submit-key {:op "_submit"})

(defn submit-failed [measurements values]
  (update measurements submit-key (partial merge-with +) {:error_count (get values :_submits)}))

(defn collect
  "Adds the given measurement (values and dimensions) to the given hashmap and
   caps the size of the hashmap at the given max-buffer-size."
  [measurements max-buffer-size dimensions values]
  (let [vals (merge {:_submits 1} values)]
    (println "collecting into" measurements)
    (if (contains? measurements dimensions)
      ; TODO: support stuff other than SUM here (e.g. AVG, MIN, MAX, etc.)
      (update measurements dimensions (partial merge-with +) vals) ; merge with existing in buffer
      (if (or (= submit-key dimensions) (< (count (dissoc measurements submit-key)) max-buffer-size))
        (assoc measurements dimensions vals) ; space available, add to buffer
        (do (println "borda buffer full, discarding measurement" dimensions vals)
            (submit-failed measurements vals)))))) ; buffer full

(defn remove-submits [[dimensions values]]
  [dimensions (dissoc values :_submits)])

(defn finalize-submit-counts [measurements]
  (let [m (update measurements submit-key merge {:success_count (->> (dissoc measurements submit-key)
                                                                     vals
                                                                     (map :_submits)
                                                                     (reduce +))})]
    (into {} (map remove-submits m))))

(defn merge-global [global-dimensions [dimensions values]]
  [(merge global-dimensions dimensions) values])

(defn merge-global-to-measurements [global-dimensions measurements]
  (into {} (map (partial merge-global global-dimensions) measurements)))

(defn go-reduce-reports [report-ch measurements-ch kill-ch max-buffer-size]
  (go-loop [measurements {}]
    (let [[v ch] (alts! [report-ch [measurements-ch measurements]])]
      (cond
        (= ch measurements-ch) (recur {})
        (nil? v) (close! kill-ch)
        :else (recur (apply collect measurements max-buffer-size v))))))

(defn flush [measurements-ch global-dimensions send on-error]
  (let [measurements (<!! measurements-ch)]
    (when (not-empty measurements)
      (try
        (send (merge-global-to-measurements global-dimensions (finalize-submit-counts measurements)))
        (catch Throwable e
          (on-error measurements e))))))

(defn go-flush-measurements [report-ch measurements-ch kill-ch global-dimensions interval send on-send-error]
  (go-loop [flush-timeout (timeout interval)]
    (let [[_ ch] (alts! [kill-ch flush-timeout])]
      (if (= ch kill-ch)
        (flush measurements-ch global-dimensions send on-send-error)
        (do (flush measurements-ch
                   global-dimensions
                   send
                   (fn [m e]
                     (on-send-error m e)
                     (onto-chan report-ch m false)))
            (recur (timeout interval)))))))

(defn reducing-submitter
  "Returns two functions. The first is a reducing submitter that collects and
   aggregates measurements and sends them using the given send function at the
   specified interval (in milliseconds from end of one send to the beginning of
   the next send) and limits the number of pending measurements to
   max-buffer-size. The second is a function that flushes the buffer and stops
   the background thread that does the submitting. Parameter on-send-error is a
   function that will be called with a map of buffered measurements and the
   error if and when flushing the buffer to borda fails. Except on the final
   flush, failed measurements will be rebuffered if space is available.
   Parameter global-dimensions is a map of dimensions that will be included with
   every single measurement."
  [global-dimensions max-buffer-size interval send on-send-error]
  (let [;; clients send reports ([dimensions values] tuples) to this channel.
        ;; These reports are immediately reduced into a single mesurements map
        ;; by `go-reduce-reports`, so writing to this channel never blocks.
        report-ch (chan)
        ;; `flush` pulls any measurements or the empty map from this channel.
        ;; Reading from this never blocks.
        measurements-ch (chan)
        ;; `go-reduce-reports` closes this channel as soon as the `report-ch`
        ;; gets closed. We do this so `go-flush-measurements` can be stopped
        ;; without `alt!`ing on`report-ch`.
        kill-ch (chan)]
    (go-reduce-reports report-ch measurements-ch kill-ch max-buffer-size)
    (go-flush-measurements report-ch measurements-ch kill-ch global-dimensions interval send on-send-error)
    [(fn [dimensions values]
       (>!! report-ch [dimensions values]))
     #(close! report-ch)]))

(defn http-sender
  "Returns a function that can be used as the 'send' parameter to
   reducing-submitter and that sends the measurements to the specified stream at
   the given URL via HTTP. connection-timeout specifies the timeout in
   milliseconds for establishing a connection to borda and request-timeout
   specifies the timeout in milliseconds for round-tripping the entire post to
   borda."
  [stream url connection-timeout request-timeout]
  (fn [measurements]
    (try+
     (-> @(http/post url
                     {:connection-timeout connection-timeout
                      :request-timeout request-timeout
                      :content-type :json
                      :body (json/generate-string (map (fn [[dimensions values]] {:name stream :dimensions dimensions :values values}) measurements))})
         :body
         bs/to-string)
     (catch [:status 400] {:keys [body]}
       (throw (Exception. (str "Bad request sending measurements to borda: " (slurp body)))))
     (catch Object _
       (throw (Exception. (str "Unexpected error sending measurements to borda: " (:throwable &throw-context))))))))
