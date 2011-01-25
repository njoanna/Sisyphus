(ns naszko.asia.zadanie4
	"Creates an index of words which length is higher then given value.
	The result is a sorted list of pairs of: words, list of files where the word occures

	Utworzenie skorowidza słów o większej niż zadana liczba liter.
	Wynikiem jest uporządkowana lista par: 
	słowo i lista nazw plików w których wyraz występuje."
	(:use (naszko.asia commons master zadania)))


(with-master

(map-reduce {

    :chunker-params [ "./teksty" ".+\\.txt" ]
    :chunker chunker-rozdziel-pliki

    
    

    :inputer inputer-odczyt-calego-pliku
   
    :partitioner rozdzielenie-modulo
    :sorter sortowanie-po-kluczu

	:mapper-params 2
    :mapper  (fncode 
				"utowrz sekwencje par: slowo z malych liter, nazwa pliku"
				[[nazwa-pliku zawartosc-pliku]]
               (let [length (*cfg* :mapper-params)]
                (filter #(> (->% first count) length)
                    (for [slowo (distinct (re-seq #"[^\W]+" zawartosc-pliku))]
                        [(.toLowerCase slowo) nazwa-pliku ]))))

    :reducer redukcja-unikaty-wartosci

	:outputer-params "./wynik/zad4-wynik_%d.txt"
    :outputer (fncode 
				"wypisz formatujac do: slowo -> plik1, plik2, plik3 ..."
				[wynik]
				(let [outname (format (*cfg* :outputer-params) *worker-id*)]
                (with-out-writer outname
                    (doseq [ [k v] wynik] (println (apply str k " -> " (interpose ", " v)) )))
					(add-counter :out (file-size outname))
					 outname))


    :reducers-num 1
    :intermediate "./temp/z4-iter-m%d-r%d-p%d"
    :buffer-size 15000 

	:tries 3	
	})


(exit))