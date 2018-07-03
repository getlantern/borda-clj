(ns borda-clj.client
  (:use [slingshot.slingshot :only [throw+ try+]])
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure.core.async :as async :refer (<! >!! <!! alts! chan close! go go-loop onto-chan timeout)]))

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
            measurements))))) ; buffer full

(defn reduce-reports [in-ch out-ch kill-ch max-buffer-size]
  (go-loop [measurements {}]
    (let [[v ch] (alts! [in-ch [out-ch measurements]])]
      (cond
        (= ch out-ch) (recur {})
        (nil? v) (close! kill-ch)
        :else (recur (apply collect measurements max-buffer-size v))))))

(defn flush [measurements-ch send on-error]
  (let [measurements (<!! measurements-ch)]
    (when (not-empty measurements)
      (try
        (send measurements)
        (catch Throwable e
          (on-error measurements e))))))

(defn flush-measurements [report-ch measurements-ch kill-ch interval send on-send-error]
  (go-loop [flush-timeout (timeout interval)]
    (let [[_ ch] (alts! [kill-ch flush-timeout])]
      (if (= ch kill-ch)
        (flush measurements-ch send on-send-error)
        (do (flush measurements-ch
                   send
                   (fn [m e]
                     (on-send-error m e)
                     (onto-chan report-ch m false)))
            (recur (timeout interval)))))))

(defn reducing-submitter
  [max-buffer-size interval send on-send-error]
  (let [;; clients send reports ([dimensions values] tuples) to this channel.
        ;; These reports are immediately reduced into a single mesurements map
        ;; by reduce-reports, so writing to this channel never blocks.
        report-ch (chan)
        ;; `flush` pulls any measurements or the empty map from this channel.
        ;; Reading from this never blocks.
        measurements-ch (chan)
        ;; `reduce-reports` closes this channel as soon as the `report-ch` gets
        ;; closed. We do this so `flush-measurements` can be stopped without
        ;; `alt!`ing on`report-ch`.
        kill-ch (chan)]
    (reduce-reports report-ch measurements-ch kill-ch max-buffer-size)
    (flush-measurements report-ch measurements-ch kill-ch interval send on-send-error)
    [(fn [dimensions values]
       (>!! report-ch [dimensions values]))
     #(close! report-ch)]))

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
