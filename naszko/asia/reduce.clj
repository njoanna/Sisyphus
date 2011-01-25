(ns naszko.asia.reduce	
	(:use (naszko.asia commons state communication stats)))


(def *completed-mappers*)
(def *intermediate-files-seq*)
(def *add-intermediate-fn*)

(defn merge-sort 
	"Funkcja zwraca sekwencje bedaca kolejnymi elementami wedlug podanego porzadku z posrod kolekcji sekwencji.
	Zaklada sie, ze sekwencje przekazane jako kolekcja maja juz uporzadkowane elementy wedle tego samego porzadku."
	[ sort-mthd seqs-to-consume ]
	(let [separate (juxt first rest)]
		(letfn [ (ordered-consume [ data ]
				(let [ [f r] (separate (sort-by first sort-mthd data))]
					(if (seq f)
						(cons (first f) (lazy-seq (ordered-consume  
							(if-let [fr (seq (rest f))]  
								(cons fr r)  r ))))))) ]  (ordered-consume (filter seq seqs-to-consume)))))


(defn lazy-group-by
	"Funkcja zwraca sekwencje bedaca para klucz grupujacy i kolekcja wartosci dla niego"
	[input]
		(for [x (partition-by first input)] 
			[(ffirst x) (map second x)]))



(defn download-files
	"Funkcja przeksztalca pare adres, nazwa pliku w nowa nazwe pliku.
	Jesli plik pod nazwa z pary nie jest widoczny na dysku, to jest on zaciagany pod nowa nazwe z podanego adresu.
	W przeciwnym wypadku nowa nazwa to nazwa z pary."
	[ [map-id address files] ]

; tutaj mozna odkomentowac kod aby miec czas ubijac jednostki do testow
		(comment do (println "\nusing slow data downloading from mapper" map-id "for tests")
		(sleep 4000))

		(if (> (get @*completed-mappers* map-id 0) 0)
			(if-let[ result (reduce (fn[final-filenames filename] 
					(if (nil? final-filenames) nil
						(let [final-filename (copy-remote-file address filename (str filename ".copied"))
;							(if (.exists (java.io.File. filename))
;							filename
;							(copy-remote-file address filename (str filename ".copied")))
							]
								(if-not (nil? final-filename) (conj final-filenames final-filename) nil )))) [] files )  ]
				(do (if (retries-exceeded-or-finished  *completed-mappers* true [] map-id)
							(@*add-intermediate-fn*))
					(doseq [final-filename result] (add-counter :in (file-size final-filename)))
						result)
				(if (retries-exceeded-or-finished *completed-mappers* false [] map-id)
					(@*add-intermediate-fn*)))))


(defn process-intermediate-files[ token files id address  ]
	(@*add-intermediate-fn* [id address files] ))

(defn process-reduce
	"Przetwarzanie reducera"
	[ token cfg reducer-id mappers-num ]

	(with-unit [cfg reducer-id ]
		  (binding [*completed-mappers* (atom (zipmap (range mappers-num) (repeat (:tries *cfg*))))]
		  (start-timer :time)
		  (let [ sortercode (loadcode :sorter) 
				 reducercode (loadcode :reducer)
				 outputercode (loadcode :outputer) ]
	
; odczytaj z dysku sekwencje par, wykonaj na nich merge-sort, natepnie pogrupuj
; pary po kluczu i wykonaj logike redukcji, zapisujac wynik do pliku

	
		  (let [ downloaded-files (doall (mapcat download-files @*intermediate-files-seq*))
				 output-filename (outputercode (mapcat reducercode  
					(lazy-group-by (merge-sort sortercode (map restore-sequence downloaded-files))))) ]
	
; zbuduj mape statystyk z przetwarzania przez tego reducera
	
				(reply-to-master "reduce-result" reducer-id token {	:time (read-timer :time)
					:in (read-counter :in)
					:out (read-counter :out)
					:data output-filename :token token}   ))))))




