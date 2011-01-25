(ns naszko.asia.state
  (:require (clojure.set)) 
	(:use (naszko.asia commons communication) (clojure pprint)))

(def *units-synch*)
(def *reported-workers*)

(defmacro with-manager[& body]
	`(binding [*units-synch* (Object.) *reported-workers* (atom {})]
		(clear-workers-existance)
		~@body))


(defn clear-workers-existance[]
	(swap! *reported-workers* (fn[workers-data] 
		{ :idle #{} :when {} :processing {} 
			:addresses {} :names { } :tokens { } })))


(defn clear-workers-state[]
	(swap! *reported-workers* (fn[workers-data] 
		(select-keys workers-data [ :idle ]) )))


(defn listen-timedout-workers[fnc]
	(schedule (bound-fn[]
		(with-local-vars [timedout nil unit-names nil]
			(swap! *reported-workers* (fn[workers-data]
					(let [older-than (now -2250) 
						which-one (filter (fn[[k v]] (< v older-than)) (:when workers-data))]
							(var-set timedout (map first which-one))
							(var-set unit-names (apply clojure.set/union 
									(vals (select-keys (:addresses workers-data) @timedout))))
							(let[new-workers-data  (apply dissoc-in workers-data [ :tokens ] @unit-names) ]	
								(apply dissoc-in new-workers-data [ :when ] @timedout))
						)))
			(if-not (empty? @timedout) 
				(do (aprintln "* timed out" (apply str (interpose " " @timedout)) "which are"
						(apply str (interpose " " @unit-names )))
				(fnc @timedout))))) 2250))


(defn report-worker-existance[ unit-address ]
	(locking *units-synch*
	(swap! *reported-workers* 
		(fn[workers-data]
			(assoc-in 
				(if (nil? (get-in workers-data [:addresses unit-address] )) 
					(update-in workers-data [:idle] conj unit-address)
						workers-data) [:when unit-address] (now))))
		(.notify *units-synch*)))


(defn get-failed-tasks-for-workers[ units-addresses ]
	(with-local-vars [unit-names nil]
		(swap! *reported-workers* (fn[ workers-data ]
			(let [new-state (apply update-in workers-data [:idle ] disj units-addresses)]
				(var-set unit-names (apply clojure.set/union (vals (select-keys (:addresses new-state) units-addresses))))
				(let [  pre-new-state (apply dissoc-in new-state [ :names ] @unit-names) 
						final-new-state (apply dissoc-in pre-new-state [ :addresses ] units-addresses) ] 

; wydrukuj stan przed odnotowaniem znikniecia i po zniknieciu, przy czym usun wpis spod klucza
; :processing tak aby nie spamowac ekranu zbednymi danymi (odkomentowac by zadzialalo)
 
					(comment aprintln "\n=====\nprzed" (with-out-str (pprint (dissoc workers-data :processing))) 
							"\n=====\npo"  (with-out-str (pprint (dissoc final-new-state :processing))) "\n======\n" )
 
						final-new-state ))))
				(for [unit-name @unit-names task (get-in @*reported-workers* [ :processing unit-name ] )]
					(apply vector unit-name task))))

(defn get-worker-address[ unit-name ]
	(get-in @*reported-workers* [:names unit-name] ))

(defn worker-exists[ unit-address ]
	(contains-in? @*reported-workers* [:when unit-address]))

(defn token-matches[ unit-name token ]
	(= token (get-in @*reported-workers* [:tokens unit-name])))

(defn reducer-is-assigned[ unit-name ]
	(contains-in? @*reported-workers* [:names unit-name] ))


(defn release-worker[ unit-name ]
	(if-let [unit-address (get-worker-address unit-name)]
		(locking *units-synch*
		(swap! *reported-workers* ( fn[ workers-data ]
			(update-in workers-data [ :idle ] conj unit-address)))
			(.notify *units-synch*))))


(defn assign-task-to-worker[ [ unit-name & what-args ] ]
	(let [ generated-token (now) unit-address	(with-local-vars [selected-unit-address nil]
			(while (nil? @selected-unit-address)
			(locking *units-synch*
			(swap! *reported-workers* (fn[ workers-data ]
				(if-let [unit-address (get-in workers-data [:names unit-name])]
						(do (var-set selected-unit-address unit-address) 
							(-> workers-data (update-in [ :idle ] disj unit-address)
								(update-in-using [ :addresses unit-address ] #{} conj unit-name)
								(update-in-using [ :processing unit-name ] #{} conj what-args )))
							(let [ [new-unit-address rest-of-idle] 
								(map-with first identity (split-set-at 1 (:idle workers-data))) ]
								(if new-unit-address (do (var-set selected-unit-address new-unit-address) 
								(-> workers-data (assoc-in [:names unit-name] new-unit-address)
								(update-in-using [ :addresses new-unit-address ] #{} conj unit-name)
								(update-in-using [ :processing unit-name ] #{} conj what-args)
								(assoc-in [ :tokens unit-name ] generated-token)
								(assoc :idle rest-of-idle))) 
									(do (.wait *units-synch*) workers-data))))))))@selected-unit-address) ]
		(let[ [what-cmd & args ] what-args]
			(aprintln "> scheduling" unit-name "to" unit-address "with token" generated-token)
			(apply execute-remote-command unit-address what-cmd generated-token args ))))


(defn reply-to-master[what-cmd & args]
	(let [ [host conn-port] (get-remote-peer)
		reply-addr (str host ":" (:master-port *cfg*)) ]
			(apply execute-remote-command reply-addr what-cmd args)))


(defn get-number-of-available-workers[]
	(sleep 2000) 
	(count (get @*reported-workers* :idle)))


(defn retries-exceeded-or-finished[ state-ref success ks worker-id ]
	(if success 
		(swap! state-ref (fn[state] (dissoc-in state ks worker-id))) 
		(swap! state-ref (fn[state] 
				(let[full-path  (conj ks worker-id) 
					state-new (update-in-using state full-path 0 dec)] 
						(if (<= (get-in state-new full-path) 0) 
							(dissoc-in state-new ks worker-id) state-new )))))
			(not (seq (get-in @state-ref ks))))



