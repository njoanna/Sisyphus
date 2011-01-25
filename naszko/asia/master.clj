(ns naszko.asia.master
	(:use (naszko.asia commons communication state stats) (clojure pprint) (clojure.contrib duck-streams)))

(def *master-port*)

(def *mappers-tasks*)
(def *add-mapper-task-fn*)

(def *reducers-tasks*)
(def *add-reducer-task-fn*)

(def *reducers-num*)
(def *completed-workers*)

(def *results*)

(defn reduce-result[reduce-id token data]
	(let [ unit-name [:reduce reduce-id] ]
		(if (token-matches unit-name token)
		(let [unit-address  (get-worker-address unit-name)]
		(aprintln "+ accepted" unit-name "with token" token)
		(aprintln "\nreducer" reduce-id unit-address (with-out-str (pprint data)))
		(swap! *results* (fn[ results ]
			(assoc-in results [ "reduce" reduce-id unit-address ] data)))
		(if (retries-exceeded-or-finished  *completed-workers* true [ :reducers ] reduce-id)
			(@*add-reducer-task-fn*)))
			(aprintln "- ignoring" unit-name "with token" token))))

(defn map-result[map-id token data]
	(let [ unit-name [:map map-id] ]
		(if (token-matches unit-name token)
			(do 
				(let [ unit-address (get-worker-address unit-name) ]
				(release-worker unit-name)
				(aprintln "+ accepted" unit-name "with token" token)
				(aprintln "\nmapper" map-id unit-address (with-out-str (pprint data)))
				(swap! *results* (fn [results ]
					(assoc-in results [ "map" map-id unit-address ] data)
					))
				(dorun (map @*add-reducer-task-fn* (map (fn[ [reduce-id files] ]
						 (vector [:reduce reduce-id] "process-intermediate-files" files map-id unit-address) ) 
								(merge (zipmap (range @*reducers-num*) (repeatedly list)) (:data data)))))))
					(aprintln "- ignoring" unit-name "with token" token))))
	
(defn reset-state[]
	(let [ [ mappers-tasks add-mapper-task-fn  ]  (pipe)
		[ reducers-tasks add-reducer-task-fn ] (pipe) ]
		(reset! *add-mapper-task-fn* add-mapper-task-fn)
		(reset! *add-reducer-task-fn* add-reducer-task-fn)
		(reset! *mappers-tasks* mappers-tasks)
		(reset! *reducers-tasks* reducers-tasks)
		(reset! *completed-workers* {})
		(reset! *results* (sorted-map))
		(reset! *reducers-num* 0)))


(defmacro with-master[& body]
	`(with-manager
		(binding [*add-mapper-task-fn* (atom nil)
					*add-reducer-task-fn* (atom nil)
					*mappers-tasks* (atom nil)
					*reducers-tasks* (atom nil)	
					*reducers-num* (atom 0)
					*results* (atom (sorted-map))
					*completed-workers* (atom {})]
			(let [ master-socket# (listen-commands 0 identity map-result reduce-result) 
				   heartbeat-socket# (receive-messages 1237 
						(fn[b# a#] (report-worker-existance (str (first (parse-address b#)) ":" a# )))) ]
			(binding [*master-port* (.getLocalPort master-socket#)]
				(println "Nasluchuje na porcie" *master-port*)
				~@body
				(clear-workers-existance)
				(locking heartbeat-socket# (.close heartbeat-socket#))
				(locking master-socket# (.close master-socket#)))))))


(defn map-reduce
  "przetwarza dane wedlug koncepcji dziel i zwyciezaj,
	do-maps zwraca sekwencję którą do-reducers konsumuje zgodnie z konfiguracją"
  [ cfg ]
		
		(with-unit [(assoc cfg :master-port *master-port*)  0]
			(clear-workers-state)
			(reset-state)
			(let [ timout (listen-timedout-workers 
					(bound-fn[ failed ]
						(let [failed-tasks (get-failed-tasks-for-workers failed)]
							(dorun (map (fn [task]
									(if (= (second task) "process-map")
										(if (retries-exceeded-or-finished  *completed-workers* false [ :mappers ] (-> task first second)) 
											(@*add-mapper-task-fn*)
												(@*add-mapper-task-fn* task))
										(if (and (= (second task) "process-reduce")
											(retries-exceeded-or-finished  *completed-workers* false [ :reducers ] (-> task first second)))
											(@*add-reducer-task-fn*)
												(@*add-reducer-task-fn* task)))) failed-tasks )))))]
		
			(elapsed "'Wykonano w' H'h' m'min' s's' S'ms'" 
				(let [ mappers-tasks-seq  (map-indexed #(vector [:map %1]
							"process-map" *cfg* %2 %1) (runcode :chunker))
						mappers-tasks-num (count mappers-tasks-seq) ]

						(doseq [mapper-task mappers-tasks-seq]
							(@*add-mapper-task-fn* mapper-task))

						(let [reducers-num (:reducers-num *cfg*)]
							(reset! *reducers-num* reducers-num)
							(reset! *completed-workers* {
									:mappers (zipmap (range mappers-tasks-num) (repeat (:tries *cfg*)))
									:reducers (zipmap (range reducers-num) (repeat (:tries *cfg*))) }))

; asynchronicznie zlecaj prace mapperom

						(bound-future
							(dorun (map assign-task-to-worker @*mappers-tasks*)))

; oraz zlecaj prace reducerom, zlecanie prac sie skonczy, jak ostatni reducer przetworzy swoje dane

						(dorun 
							(map (fn[ reducer-task ] 

; jesli jeszcze nie ma powolanego reducera, to zrob to teraz

								(let [reducer-name (first reducer-task)]
							
									(if-not (reducer-is-assigned reducer-name)
										(assign-task-to-worker [reducer-name "process-reduce" 
											*cfg* (second reducer-name) mappers-tasks-num])))
								
; a nastepnie zlec mu grupe plikow
								
								(if-not (= (second reducer-task) "process-reduce")
									(if (worker-exists (last reducer-task))
										(assign-task-to-worker reducer-task))))
 
									@*reducers-tasks*))


; teraz wypisz statystyki
	
						(let [res (with-out-str
							(aprintln (format "%-10s %-15s %-15s %-20s %-10s %-10s" "praca" 
															"rozmiar danych" "rozmiar danych" 
																"adres procesu" "czas" "token"))
												
							(aprintln (format "%-10s %-15s %-15s %-20s %-10s %-10s" "workera" 
															"wejsciowych" "wyjsciowych" 
																"workera" "wykonania" ""))
	
							(dorun (for [ [type data1] @*results* [ id data2 ] (sort-by first data1) [address data3] data2 ]
								(aprintln (format "%-10s %-15s %-15s %-20s %-10s %-10s"
									 (str type " " id) (:in data3)
										 (if-let [out (:out data3)] out "-") address (:time data3) (:token data3)  )))))]

								(spit (format "wynik_%1$tY%1$tm%1$td-%1$tH%1$tM%1$tS.txt"
										 (java.util.GregorianCalendar.)) res)
								(aprintln res))


						(comment aprintln (with-out-str (pprint @*results*)))
					
						(.interrupt timout))))))


