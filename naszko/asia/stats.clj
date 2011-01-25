(ns naszko.asia.stats
	(:use (naszko.asia commons)))


(def *stats*)


(defn read-counters[]
	(:counters *stats*))

(defn read-timers[]
	(:timers *stats*))


(defn read-counter[name]
	(get-in @*stats* [ :counters name ]))

(defn set-counter[name value]
	(swap! *stats* (fn[a] (assoc-in a [ :counters name ] value))))

(defn add-counter[name value]
	(swap! *stats* (fn [a] (assoc-in a [ :counters name ]
		(if-let [x (get-in a [ :counters name ])]
			(+ x value) value)))))




(defn stop-timer[name]
	(swap! *stats* (fn[a]
	(let [n (now) [f s] (get-in a [ :timers name ])]
		(if (> s 0)
			(assoc-in a [ :timers name ] [(+ f (- n s)) 0]) a)))))

(defn reset-timer[name]
	(swap! *stats* (fn[a] (assoc-in a [ :timers name ] [0 0]))))

(defn read-timer[name]
	(if-let [ [f s] (get-in @*stats* [ :timers name ])]
			(if (> s 0) (+ f (- (now) s)) f) 0))

(defn start-timer[name]
	(swap! *stats* (fn[a]
	(let [[f s] (or (get-in a [ :timers name ]) [0 0])]
		(if (= s 0) (assoc-in a [ :timers name ] [f (now)]) a)))))



(defmacro with-stats[& body]
	`(binding [*stats* (atom {:timers {} :counters {}})]
		~@body))


(defmacro with-unit[ [cfg unit] & body]
	`(binding [ *cfg* ~cfg *worker-id* ~unit]
			 (with-stats ~@body)))


(comment with-stats
	(start-timer :kozak)
	(sleep 1000)
	(start-timer :kozak)
	(sleep 1000)
	(println (read-timer :kozak))
	(sleep 1000)
	(println (read-timer :kozak))
	(stop-timer :kozak)
	(sleep 1000)
	(println (read-timer :kozak))
	(start-timer :kozak)
	(sleep 1500)
	(println (read-timer :kozak))
	
		)


