(ns naszko.asia.commons
	(:use (clojure.contrib io) (clojure set)))

(def *cfg*)
(def *worker-id*)


(defn pipe
	"Otwiera punkt synchronizacyjny dla watkow, zwracajac pare w ktorej pierwszy element
	to sekwencja produktow, a drugi to funkcja ktora gdy wykonana z jednym argumentem to
	doklada do sekwencji produktow, a gdy wywolana bez argumentow to zamyka sekwencje pozwalajac
	tym samym na jej calkowita konsumpcje. Do pipe mozna przekazac col bedacy stanem poczatkowym sekwencji."
 ([] (pipe nil))
 ([col]
 (let [q (java.util.concurrent.LinkedBlockingQueue.) s (Object.)]
   [(concat col (take-while (partial not= q) (repeatedly #(let[e (.take q)] (if (= e s) nil e)))))
    	(fn ([e] (.put q (if (nil? e) s e))) 
			([] (.put q q)))])))


(defmacro bound-future[ & body ]
	"Wykonuje future w kontekscie zmiennych obecnego watku"
	`(future-call (bound-fn [] ~@body)))

(defmacro bound-thread[ & body ]
	"Wykonuje watek z maksymalnym priorytetem w kontekscie zmiennych obecnego watku"
	`(let [ fnc# (bound-fn [] ~@body)]
		(doto (Thread. (reify Runnable (run[this] (fnc#) ))) 
			(.setPriority Thread/MAX_PRIORITY) (.start))))

(defn file-size
	"Zwraca wielkosc pliku"
	[path]
	(.length (java.io.File. path)))


(defn now
  "Zwraca czas dla teraz. Można podać argument, który będzie dodany do czasu obecnego
  (np. może to być -1000 czyli, jedna sekunda wcześniej od teraz)"
  ([] (System/currentTimeMillis))
  ([n] (+ (now) n)))


(defn persist-sequence
  "Zapisuje sekwencje do pliku z uwzględnieniem struktur"
  [path seqence-to-store]
	(binding [ *out* (java.io.PrintWriter. (java.io.FileOutputStream. path))]
			(doseq [d seqence-to-store] (prn d)) (.close *out*)))

(defn restore-sequence
  "Odczytuje sekwencje z pliku z uwzględnieniem struktur"
  [path]
	(let [reader (java.io.PushbackReader. 
		(java.io.InputStreamReader. 
			(java.io.FileInputStream. path)))]
	(letfn [(recur-reading [in]
			(if-let [r (read in false nil)]
				(cons r (lazy-seq (recur-reading in))) 
					(.close in)))] (recur-reading reader) )))


(defn re-pat
  "Tworzy wzorzec wyrazenia regularnego"
  [ & args]
  (java.util.regex.Pattern/compile (apply str args)))


(defmulti log-error class)

(defmethod log-error Exception [e]
	(println "Reporting catched error:" e)
	(.printStackTrace e))

(defmethod log-error String [e]
	(println "Reporting message error:" e))


(defn sleep
	([] (.join (Thread/currentThread)))
	([a] (Thread/sleep a))
	([a b] (Thread/sleep (+ a (rand-int (- b a))))))


(defn exit 
	([] (exit 0))
	([n] (System/exit n)))


(defmacro defncode 
	"Nie da sie przyslac funkcji przez siec (nie sa serializowalne)
	 Dlatego kod, ktory wyglada jak funkcja trzeba zamienic poprostu na napis,
     przeslac go przez sieci i po drugiej stronie zinterpretowac jako funkcje"
	[codename & body] `(def ~codename (fncode ~@body)))

	
(defmacro fncode
	"Tak jak defcode ale bez definiowania nazwy"
	[ & body] `(prn-str (quote (fn ~@(if (= String (class (first body)) ) (rest body) body )   ))))


(defn loadcode 
	"Interpretowanie napisow przeslanych przez siec do postaci funkcji"
	[codename]
		(if-let [code-as-string (codename *cfg*)]
			(eval (load-string code-as-string))))

(defn runcode 
	"Interpretowanie napisow przeslanych przez siec do postaci funkcji i wykonanie tej funkcji"
	[codename & data]
		(apply (loadcode codename) data))


(defmacro elapsed
	"Pomiar czasu wykonania. Pomiar jest od razu wypisany na ekran"
  [fmt expr]
  `(let [start# (System/currentTimeMillis)
         ret# ~expr stop# (- (System/currentTimeMillis) start#)]
     (println (.format (doto (java.text.SimpleDateFormat. ~fmt) 
			(.setTimeZone (java.util.TimeZone/getTimeZone "GMT:00")) )
		 			(java.util.Date. stop#)))
     ret#))

(let [pm (Object.)]
	(defn aprintln 
		"Synchroniczne wypisywanie na ekran, przydatne przy wielu watkach
		wypisujacych cos na ekran"
		[ & args ]
		(locking pm
			(apply println args))))

(defn dissoc-in 
	"Przez analogie do assoc-in oraz update-in, idzie po zagniezdzonej
	mapie, by na koncu drogi usunac wartosci z pod kolekcji kluczy keys-to-remove"
	[ data keys & keys-to-remove ]
		(if (seq keys)
			(reduce #(assoc-in %1 keys 
				(dissoc (get-in %1 keys) %2)) data keys-to-remove)  
			(apply dissoc data keys-to-remove)))

(defmacro update-in-using
	"Tak jak update-in tylko, ze pozwala na zdefiniowanie ostatniego wiazania
	w przypadku gdy ono nie istnieje - domyslnie bylaby to hashmapa, a tak mozemy sami
	zadecydowac co to ma byc"
	[ data keys ele op & args ]
		`(if-let [s# (get-in ~data ~keys)] 
			(update-in ~data ~keys ~op ~@args)
			(-> ~data (assoc-in ~keys ~ele)
				(update-in ~keys ~op ~@args))))


(defn contains-in?[ m keys ]
	(not (nil? (get-in m keys))))

(defn update-in-get
	"Robi update i zwraca pare: mapa po aktualizacja i wartosc elementu zaktualizowanego"
	[ data keys & op-and-args ]
		(let [new-data (apply update-in data keys op-and-args)
				 new-value (get-in new-data keys) ]
				[new-data new-value]))

(defn split-set-at
	"Dzieli zbior na dwa, przy czym at wyznacza wielkosc pierwszego zbioru"
	[ at set ]
		(let [ [left right] (split-at at set) ]
		 [ (into #{} left) (into #{} right) ]))


(defn map-with
	"Bierze operacje oraz sekwencje, nastepnie dla kolejnych elementow z sekwencji
	wykonuje odpowiadajaca im operacje i zwraca wektor"
	[ & args ]
		(map #(%1 %2) (cycle (butlast args)) (last args)))


(defn parallel-map 
	"Zleca wszystkie zadania na raz, potem czeka, az wszystkie sie wykonaja
	i zwraca ich wynik. W przeciwienstwie do pmap nie zalezy
	o liczby procesorow i nie musi, gdyz praca jest zlecana procesom zewnetrznym
	nie koniecznie znajdujacym sie na tym samym komputerze"
	[f & q]
	(let [job-to-do (bound-fn[x z] (apply x z) )]
	 (let [agents (reduce (fn[ c  z] (let [a (agent f)]
		(send-off a job-to-do z) (conj c a))) [] (apply map vector q) )]
		(apply await agents)
		(seq (reduce (fn[a b] (conj a (deref b))) [] agents)))))


(defn schedule
	"Odpala zadanie cykliczne, mozna je zatrzymac 
	wywolujac .interrupt na jego referencji"
	[fnc frequency]
	(let [a (atom nil)]
		(reset! a (Thread. (reify Runnable (run[this]
			(try
			(while (not (.isInterrupted @a))
			(fnc)
			(try (sleep frequency)(catch InterruptedException e))) 
				(catch Exception e (comment log-error e)))
				)))) (.start @a) @a))

