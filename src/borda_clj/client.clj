(ns borda-clj.client
  (:use [slingshot.slingshot :only [throw+ try+]])
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :as json]))

(def discard_key {:op "_discard"})

(defn empty-measurements []
  {discard_key {:success_count 0}})

(defn collect
  "Adds the given measurement (values and dimensions) to the given hashmap and caps the size of the hashmap at the given max-buffer-size"
  [measurements max-buffer-size dimensions values]
  (let [key (into (sorted-map) dimensions)]
    (if (contains? measurements key)
      ; TODO: support stuff other than SUM here (e.g. AVG, MIN, MAX, etc.)
      (update measurements key (partial merge-with +) values) ; merge with existing in buffer
      (if (< (count measurements) max-buffer-size)
        (assoc measurements key values) ; space available, add to buffer
        (do (print "borda buffer full, discarding measurement" dimensions values)
            (update measurements discard_key (partial merge-with +) {:success_count 1})))))) ; buffer full

(defn reducing-submitter
  "Returns two functions. The first is a reducing submitter that collects and
   aggregates measurements and sends them using the given send function at the
   specified interval (in milliseconds from end of one send to the beginning of
   the next send) and limits the number of pending measurements to
   max-buffer-size. The second is a function that flushes the buffer and stops
   the background thread that does the submitting. Parameter on-send-error is a
   function that will be called with a map of buffered measurements and the
   error if and when flushing the buffer to borda fails. Except on the final
   flush, failed measurements will be rebuffered if space is available."
  [max-buffer-size interval send on-send-error]
  (let [measurements  (atom (empty-measurements))
        running       (atom true)
        submit        (fn [dimensions values] (swap! measurements collect max-buffer-size dimensions values))
        resubmit      (fn [m] (doseq [[dimensions values] m] (submit dimensions values)))
        flush         (fn [on-flush-error]
                        (let [[m] (reset-vals! measurements (empty-measurements))]
                          (if (not-empty m)
                            (try
                              (send m)
                              (catch Throwable e
                                (on-flush-error m e))))))
        stop          (fn [] (reset! running false) (flush on-send-error))]
        ; periodically send to borda
    (future (while @running (do
                              (Thread/sleep interval)
                              (flush (fn [m e] (on-send-error m e) (resubmit m))))))
    [submit stop]))

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
