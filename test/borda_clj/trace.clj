(ns borda-clj.trace
  (:require  [clojure.test :as t]
             [borda-clj.client :as sut]
             [clojure.core.async :as async :refer (<! <!! >! >!! alts! chan close! go go-loop onto-chan timeout)]
             [clojure.spec.alpha :as s]
             [clojure.spec.gen.alpha :as gen]
             [clojure.spec.test.alpha :as stest]
             [borda-clj.trace :as trace]
             [clojure.pprint :as pp :refer (pprint)]))

(defn run-trace [global-dimensions max-buffer-size trace]
  (let [[report-ch measurements-ch kill-ch timeout-ch trace-ch] (repeatedly chan)
        successful-send (fn [m] (>!! trace-ch {::send m}))
        failed-send (fn [& args] (throw (RuntimeException. "boom")))
        send (fn [measurements] ((or (:trace-metadata (meta measurements))
                                    ;; it doesn't matter much whether we succeed
                                    ;; or fail in the last sending attempt,
                                    ;; since we never retry that one. it
                                    ;; slightly simplifies tests to assume it
                                    ;; succeeds, so let's do that.
                                    successful-send)
                                measurements))
        on-send-error (fn [m e]
                        ;;(clojure.pprint/pprint e)
                        (>!! trace-ch {::error m}))
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

(s/def ::key simple-keyword?)
(s/def ::finite-number (s/or :int int? :double (s/and double? #(Double/isFinite %))))
(s/def ::dimensions (s/every-kv ::key
                                (s/or :str (s/and string? not-empty) :num ::finite-number)
                                :min-count 1))
(s/def ::values (s/every-kv ::key ::finite-number
                            :min-count 1))
(s/def ::measurement (s/tuple ::dimensions ::values))
(s/def ::measurements (s/map-of ::dimensions ::values))
(s/def ::trace-input-entry (s/or :kw #{:successful-tick :failed-tick :delay} :measurement ::measurement))
(s/def ::input-trace (s/coll-of ::trace-input-entry :max-count 10))
(s/def ::send ::measurements)
(s/def ::final-send ::measurements)
(s/def ::error ::measurements)
(s/def ::trace-output-entry (s/and (s/or :send (s/keys :req [::send])
                                         :error (s/keys :req [::error]))
                                   ;; this predicate runs on the conformed value
                                   #(= (count (second %)) 1)))
(s/def ::output-trace (s/coll-of ::trace-output-entry))

(defn gen-run-trace-args
  "A generator that creates measurements with enough collisions of dimensions
  and values to trigger accumulation."
  []
  (gen/bind
   (s/gen (s/int-in 1 5))
   (fn [n]
     (gen/bind
      (gen/tuple
       (gen/return (gen/sample (s/gen ::dimensions) n))
       (gen/return
        (gen/sample
         (gen/bind (s/gen (s/int-in 1 4))
                   (fn [num-vals]
                     (gen/return (gen/sample (s/gen simple-keyword?) num-vals))))
         4)))
      (fn [[frequent-dimensions frequent-value-keysets]]
        (gen/tuple
         (s/gen ::dimensions)
         (gen/return n)
         (gen/vector
          (gen/frequency
           [[90 (gen/tuple
                (gen/frequency [[2 (gen/elements frequent-dimensions)] [1 (s/gen ::dimensions)]])
                (gen/frequency [[9 (gen/bind
                                    (gen/elements frequent-value-keysets)
                                    (fn [keyset]
                                      (gen/bind
                                       (gen/vector (s/gen number?) (count keyset))
                                       (fn [nums]
                                         (gen/return
                                          (into {}
                                                (map vector keyset nums)))))))]
                                [1 (s/gen ::values)]]))]
            [1 (s/gen ::trace-input-entry)]]))))))))

(s/def ::run-trace-args
  (s/spec (s/cat :global-dimensions ::dimensions :max-buffer-size pos-int? :trace ::input-trace)
          :gen gen-run-trace-args))

(defn expect-some-output
  "If we send any measurements, we expect some output"
  [x]
  (if (some #(= (first %) :measurement) (-> x :args :trace))
    (not-empty (-> x :ret))
    true))

(defn map-in? [m1 m2]
  (= m1 (select-keys m2 (keys m1))))

(defn globals-are-in-all-sends [{:keys [ret] {:keys [global-dimensions]} :args}]
  (every? (fn [{{measurements ::send} :send}]
             (if (some? measurements)
               (every?
                (fn [k]
                  (map-in? global-dimensions k))
                (keys measurements))
               true))
          (seq ret)))

(defn total-reported [global-dimensions trace]
  (->> trace
       (filter #(= (first %) :measurement))
       (map second)
       (reduce (fn [m [d v]]
                 (update m (merge global-dimensions d) (partial merge-with +) v))
               {})))

(defn total-sent [global-dimensions result]
  (->> result
       (filter #(= (first %) :send))
       (map second)
       (map ::send)
       (reduce (partial merge-with (partial merge-with +)))
       (#(dissoc % (merge global-dimensions sut/submit-key)))))

(defn sent<=reported
  "The total quantity of successfully sent data in each dimension is less than
  or equal than the total quantity we reported."
  [{:keys [ret] {:keys [global-dimensions trace]} :args}]
  (let [reported (total-reported global-dimensions trace)
        sent (total-sent global-dimensions ret)]
    (every? (fn [k]
              (let [reported-vals (get reported k)
                    sent-vals (get sent k)]
                ;; XXX: this also passes if we change <= to =, which I suspect
                ;; reflects a problem with our test data generator
                (every? #(<= (% sent-vals) (% reported-vals))
                        (keys sent-vals))))
            (keys sent))))

(defn delays-dont-affect-result
  "Unfortunately this is not true. The output stream is not a deterministic
  function of the input stream. In particular, ticks can see reports that
  happened after them. This doesn't make our logic incorrect, only less
  straightforward to test."
  [x]
  (= (->> x :ret (s/unform ::output-trace))
     (run-trace (->> x :args :global-dimensions (s/unform ::dimensions))
                (-> x :args :max-buffer-size)
                (->> x :args :trace (s/unform ::input-trace) (remove #{:delay})))))

(s/fdef
 run-trace
 :args ::run-trace-args
 :ret ::output-trace
 :fn (s/and expect-some-output
            globals-are-in-all-sends
            sent<=reported))


(comment

  (stest/abbrev-result (first (stest/check `run-trace {:clojure.spec.test.check/opts {:num-tests 1000 :max-size 5}})))

  (clojure.pprint/pprint
   (run-trace {:g "global"}
              2
              [[{:d "d"} {:v 1}]
               [{:e "e"} {:v 1}]
               [{:f "f"} {:v 1}]
               :failed-tick]))

  (def sent *1)
  (def p (nth (s/exercise ::run-trace-args 20) 19))
  (def raw-input (first p))
  (def conformed-input (second p))
  (def raw-output (apply run-trace raw-input))
  (def conformed-output (s/conform ::output-trace raw-output))
  (def x {:args conformed-input :ret conformed-output})
  (def ret conformed-output)
  (def global-dimensions (:global-dimensions conformed-input))
  (def trace (:trace conformed-input))

  )
