(ns borda-clj.client
  (:use [slingshot.slingshot :only [throw+ try+]])
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :as json]))

(def submit-key {:op "_submit"})

(defn submit-failed [measurements values]
  (update measurements submit-key (partial merge-with +) {:error_count (get values :_submits)}))

(defn collect
  "Adds the given measurement (values and dimensions) to the given hashmap and
   caps the size of the hashmap at the given max-buffer-size."
  [measurements max-buffer-size dimensions values]
  (let [key  (into (sorted-map) dimensions)
        vals (update values :_submits (fnil max 0 0) 1)] ; use existing _points if available, else 1
    (if (contains? measurements key)
      (do
        ; TODO: support stuff other than SUM here (e.g. AVG, MIN, MAX, etc.)
        (update measurements key (partial merge-with +) vals)) ; merge with existing in buffer
      (if (< (count measurements) max-buffer-size)
        (assoc measurements key vals) ; space available, add to buffer
        (do (println "borda buffer full, discarding measurement" key vals)
            (submit-failed measurements vals)))))) ; buffer full

(defn remove-submits [[dimensions values]]
  [dimensions (dissoc values :_submits)])

(defn finalize-submit-counts [measurements]
  (let [m (update measurements submit-key merge {:success_count (reduce + (map (fn [val] (get val :_submits)) (vals measurements)))})]
    (into {} (map remove-submits m))))

(defn merge-global [global-dimensions [dimensions values]]
  [(merge global-dimensions dimensions) values])

(defn merge-global-to-measurements [global-dimensions measurements]
  (into {} (map (partial merge-global global-dimensions) measurements)))

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
  (let [measurements  (atom {})
        running       (atom true)
        submit        (fn [dimensions values] (swap! measurements collect max-buffer-size dimensions values))
        resubmit      (fn [m] (doseq [[dimensions values] m] (submit dimensions values)))
        flush         (fn [on-flush-error]
                        (let [[m] (reset-vals! measurements {})]
                          (if (not-empty m)
                            (try
                              (send (merge-global-to-measurements global-dimensions (finalize-submit-counts m)))
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
