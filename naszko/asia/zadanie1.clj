(ns naszko.asia.zadanie1
	"Number of occurences of a particular word in a set of text documents.
	The result is a pair of: searched word, its number of occurences.

	Zliczanie konkretnego słowa ze zbioru plików tekstowych.
 	Wynikiem jest para: szukane słowo i liczba wystąpienia słowa."
	(:use (naszko.asia commons master zadania)))


(with-master

(defncode mapowanie-wyliczanka-slowa
  "dla zadanego slowa, przeksztalc na sekwencje par: to slowo, wartosc jeden"
  [[nazwa-pliku zawartosc-pliku]]
    (let [word (:mapper-params *cfg*)]
        (filter #(= word (first %))
            (for [slowo (re-seq #"[^\W]+" zawartosc-pliku)] 
				[(.toLowerCase slowo) 1]))))


(def konfiguracja {
           
    :chunker-params [ "./teksty" ".+\\.txt" ]
    :chunker chunker-rozdziel-pliki

    

    :inputer inputer-odczyt-calego-pliku

    :outputer outputer-zapis-wartosci-sekwencji-par
	:outputer-params "./wynik/zad1-wynik_%d.txt"
    

    :sorter sortowanie-po-kluczu

	:mapper-params "ipsum"
    :mapper  mapowanie-wyliczanka-slowa

    
    :reducer redukcja-podsumowanie

	:combiner redukcja-podsumowanie

    :partitioner rozdzielenie-modulo

    :reducers-num 1
    :intermediate "./temp/z1-iter-m%d-r%d-p%d"
    :buffer-size 15000 
	
	:tries 3

	})

(map-reduce konfiguracja)

(exit))







