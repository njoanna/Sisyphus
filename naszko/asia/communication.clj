
(ns naszko.asia.communication
	(:use (naszko.asia commons) (clojure.contrib io) )
	(:import [java.net DatagramPacket DatagramSocket 
			InetSocketAddress InetAddress Socket ServerSocket] 
		[java.io BufferedReader ByteArrayOutputStream 
			ByteArrayInputStream InputStreamReader PrintStream]))

(def *remote-peer*)

(defn parse-address[ addr ]
	(let [ [x y] (.split addr ":") ]
		[x (Integer/parseInt y)]))

(defn- create-connection[addr]
	(let [ [host port ] (parse-address addr) sck (Socket. (InetAddress/getByName host) port) ]
		[(.getInputStream sck) (PrintStream. (.getOutputStream sck))]))

(defn copy-remote-file [addr requested-file destination-filename]
	(try
		(let [ [in out] (create-connection addr) ]
			(.println out "file")
			(.println out requested-file)
			(.flush out)
			(copy in (java.io.File. destination-filename))
				destination-filename)
					(catch Exception e nil)))

(defn execute-remote-command [addr cmd & data]
	(try
		(let [ [ins out] (create-connection addr)
			in (BufferedReader. (InputStreamReader. ins)) ]
			(.println out cmd)
			(.println out (prn-str data))
			(.flush out)
			(read-string (.readLine in))) 
				(catch Exception e nil)))


(defn- exec-behaviour[fc]
	(fn [in out]
		(.println out (prn-str (apply fc 
			(read-string (.readLine in)))))(.flush out)))

(defn- asynch-exec-behaviour[fc]
	(fn [in out]
		(let [args (read-string (.readLine in))]
		(.println out (prn-str nil))
		(.flush out)
		(apply fc args))))

(defn- copy-behaviour[in out]
	(copy (java.io.FileInputStream. (.readLine in)) out)
			(.flush out))

(defn get-remote-peer[]
	(parse-address *remote-peer*))

(defn listen-commands[ port fnc & commands ]
	(let [cmds (assoc (zipmap (map #(str (:name (meta %1))) commands ) 
			(map asynch-exec-behaviour commands)) "file" copy-behaviour)]
	(let [s1 (ServerSocket. port)]
		(bound-future (while (not (.isClosed s1))
					(let [c1 (.accept s1)]
						(bound-future (binding [*remote-peer* 
							(str (.getHostName (.getInetAddress c1)) ":" (.getPort c1) )]
							(try
							(let [in (BufferedReader. (InputStreamReader. (.getInputStream c1)))
								  out (PrintStream. (.getOutputStream c1))
								  cmdname (.readLine in)]
								(if-let [ fc (get cmds cmdname)]
									(if-let [ok (fnc cmdname)]
									(fc in out)
									(throw (Exception. (str "unknown command: " cmdname)))
								)))(catch Exception e2 (log-error e2))(finally (.close c1)))))
											))) s1)))









(defn send-message[addr freqency fnc]
	(let [ [host port ] (parse-address addr) ]
	(let [ms (DatagramSocket.) adr (InetAddress/getByName host) baos (ByteArrayOutputStream. 512)]
		(bound-thread
		(while (not (.isClosed ms))
			(try
			(.reset baos)
			(let [out (PrintStream. baos)]
				(.println out (prn-str (fnc)))
				(.flush out)
				(let [ba (.toByteArray baos) dp (DatagramPacket. ba 0 (count ba) adr port)]
				  (locking ms (if-not (.isClosed ms)	(.send ms dp) )  )))
						(catch Exception e))(Thread/sleep freqency)))  ms)))


(defn receive-messages[port fnc]
	(let [ms (DatagramSocket. port) ba (byte-array 1024)]
		(.setSoTimeout ms 2500)
		(bound-thread
			(while (not (.isClosed ms))
				(try
				(let [dp (DatagramPacket. ba (count ba))]
			(locking ms (if-not (.isClosed ms) (.receive ms dp)))
			(fnc (str (.getHostAddress (.getAddress dp)) ":" (.getPort dp) )
				(read-string (.readLine 
					(BufferedReader. (InputStreamReader.
						 (ByteArrayInputStream. (.getData dp) 0 (.getLength dp))))))  )) 
						(catch Exception e)))) ms ))





