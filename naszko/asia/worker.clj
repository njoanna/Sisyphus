(ns naszko.asia.worker
	"Proces uruchamia nasłuch na funkcje 
	wyknania logiki dla map i reduce"
	(:use (naszko.asia commons communication map 
		reduce zadania stats) (clojure.contrib io)))

(defn reset-queues[]
	(let [[intermediate-files-seq add-intermediate-fn] (pipe)]
		(reset! *intermediate-files-seq* intermediate-files-seq)
		(reset! *add-intermediate-fn* add-intermediate-fn)))

(binding [*intermediate-files-seq* (atom nil)
		  *add-intermediate-fn* (atom nil)]	
	(with-open [worker-socket (listen-commands 
		(Integer/parseInt (or (first *command-line-args*) "0"))
		(fn[req-cmd] (if (contains? #{"process-reduce" "process-map"} req-cmd) (reset-queues)) 
				(print (condp = req-cmd "process-reduce" "r"
				"process-map" "m" "process-intermediate-files" "i" "file" "t" "d")) true)
			 process-reduce process-map process-intermediate-files)
					 worker-port (.getLocalPort worker-socket)]
	
		(println "Nasluchuje na porcie" worker-port)
		; 127.0.0.1
		; 192.168.1.255
		(send-message "127.0.0.1:1237" 1000 
			(fn[] (print ".") (flush) worker-port))
	
		(sleep)))