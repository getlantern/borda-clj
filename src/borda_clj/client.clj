(ns borda-clj.client
  (:use [slingshot.slingshot :only [throw+ try+]])
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :as json]))

(defn collect
  "Adds the given measurement (values and dimensions) to the given hashmap and caps the size of the hashmap at the given max-buffer-size"
  [measurements max-buffer-size dimensions values]
  (let [key (merge (sorted-map) dimensions)]
    (if (contains? measurements key)
      ; TODO: support stuff other than SUM here (e.g. AVG, MIN, MAX, etc.)
      (assoc measurements key (merge-with + (get measurements key) values)) ; merge with existing in buffer
      (if (< (count measurements) max-buffer-size)
        (assoc measurements key values) ; space available, add to buffer
        (do (print "borda buffer full, discarding measurement" dimensions values) measurements))))) ; buffer full

(defn reducing-submitter
  "Returns two functions. The first is a reducing submitter that collects and
   aggregates measurements and sends them using the given send function at the
   specified interval (in milliseconds) and limits the size of pending
   measurements to max-buffer-size. The second is a function that flushes the
   buffer and stops the background thread that does the submitting."
  [max-buffer-size interval send on-send-error]
  (let [measurements  (atom (hash-map))
        running       (atom true)
        submit        (fn [dimensions values] (swap! measurements collect max-buffer-size dimensions values))
        resubmit      (fn [m] (doseq [[dimensions values] (seq m)] (submit dimensions values)))
        flush         (fn [on-flush-error]
                        (let [[m] (reset-vals! measurements (hash-map))]
                          (if (> (count m) 0)
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
   the given URL via HTTP"
  [stream url]
  (fn [measurements socket-timeout conn-timeout]
    (try+
      (-> @(http/post url
                      {:socket-timeout socket-timeout
                       :conn-timeout conn-timeout
                       :content-type :json
                       :body (json/generate-string (map (fn [[dimensions values]] {:name stream :dimensions dimensions :values values}) measurements))})
          :body
          bs/to-string)
      (catch [:status 400] {:keys [request-time headers body]}
        (throw (Exception. (str "Bad request sending measurements to borda: " (slurp body)))))
      (catch Object _
        (throw (Exception. (str "Unexpected error sending measurements to borda: " (:throwable &throw-context))))))))
