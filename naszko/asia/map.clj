(ns naszko.asia.map
	(:use (naszko.asia commons stats state reduce)))


(defn process-map
	"Wykonuje logike mappera"
	[ token cfg chunker-task mapper-id ]


;	(aprintln (with-out-str (clojure.pprint/pprint cfg)))

; wczytaj konfiguracje z cfg, zrob tworce nazw plikow posrednich oraz
; utworz logike rozdzielania par z maperow pomiedzy reducerow

	(with-unit [ cfg mapper-id ]
	  (start-timer :time)
	  (let [mappercode (loadcode :mapper) 
			buffer-size (:buffer-size cfg)
			inputercode (loadcode :inputer)
			reducers-num (:reducers-num cfg)
			sortercode (loadcode :sorter)
			combinercode (loadcode :combiner)
			partitionercode (loadcode :partitioner)
			filename-pattern (:intermediate cfg)]
	
; nastepnie wykonaj logike mappera - wejsciem do funkcji mappera jest sekwencja
; utworzona przez imputera, wynik wykonania to jeden wielki ciag wartosci par z sekwencji
; zwracanaych przez kolejne wywolania mappera
; na koniec rozdzielamy ten wielki ciag na tyle czesci ile jest reducerow
; podczas podzialu, jesli dana czesc przekroczy liczbe elementow okreslona w konfiguracji zadania
; wtedy cala ta czesc jest sortowana i zapisywan na dysku (tak aby nieprzekroczyc zurzycia pamieci)
; gdy calosc sie wykona to jeszcze wcisnij na dysk pozostalosci z podzialu 
; (jesli nie przekroczyly zadanej liczby z konfiguracji
; zadania, wtedy istnieja w pamieci, dlatego zmuszamy je do zapisu na dysk) 

		(let [ output-fn (fn [reducer-id [index pair-seq] postfix] 
							(let [ output-name (format (str filename-pattern postfix) mapper-id reducer-id index)]
							(persist-sequence output-name pair-seq)
							(add-counter :out (file-size output-name)) 
								output-name))

			   processed-seq (mapcat mappercode (inputercode chunker-task))

; Bierze ciag wejsciowy, rozdziela wedlug funkcji uzytkownika pomiedzy czesci dla reducerow,
; paczkuje w zadana liczbe, nastepnie sortuje paczke, zapisuje wynik, a nazwe pliku zapamietuje	
; w sekwencji ktora zwraca jako mape par: numer reducera, sekwencja jego plikow"

			   intermediate-files  (apply merge-with into (parallel-map 
							(bound-fn[ reducer-id ]
								(reduce (fn[data indexed-pair] (update-in data [ reducer-id ] conj (output-fn reducer-id indexed-pair ""))) {}
									(map-indexed (fn[index pair-seq] [index (sort sortercode pair-seq) ])
									    (partition-all buffer-size (filter (fn[ pair ] 
											(= reducer-id (partitionercode reducers-num pair))) processed-seq )))))
												(range reducers-num))) ]

; wykonaj jeszcze opcjonalna kombinacje, aby zredukowac wyniki czastkowe dla reducerow
				
			(let[ combined-intermediate-files (if combinercode
					(apply merge-with into (map (fn[[reducer-id reducer-files]] 
						(reduce (fn[data indexed-pair] (update-in data [reducer-id] conj (output-fn reducer-id indexed-pair ".cmb"))) {}
						  	(map-indexed vector (partition-all buffer-size 
								(mapcat combinercode  
									(lazy-group-by (merge-sort sortercode (map restore-sequence reducer-files)))))))) intermediate-files)) 
										intermediate-files) ]

; zbuduj mape statystyk z przetwarzania przez tego mappera i odeslij masterowi
							(reply-to-master "map-result" mapper-id token { :time (read-timer :time)
									:in (read-counter :in)
									:out (read-counter :out)
									:data combined-intermediate-files :token token} )  )))))






