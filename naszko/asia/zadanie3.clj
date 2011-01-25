(ns naszko.asia.zadanie3
	"Sorting of words from a set of text documents.
	The results is a set of sorted unique words.

	Sortowanie słów ze zbioru plików tekstowych.
	Wynikiem jest uporządkowana lista unikatowych wyrazów."
	(:use (naszko.asia commons master zadania)))

(with-master

(def konfiguracja {

    :chunker-params [ "./teksty" ".+\\.txt" ]
    :chunker chunker-rozdziel-pliki




    :inputer inputer-odczyt-calego-pliku

    :outputer-params "./wynik/zad3-wynik_%d.txt"
    :outputer outputer-zapis-wartosci-sekwencji

    :sorter sortowanie-po-kluczu

    :mapper  mapowanie-wyliczanka

    :reducer redukcja-klucze

	:combiner redukcja-unikaty

    :partitioner rozdzielenie-modulo

    :reducers-num 1
    :intermediate "./temp/z3-iter-m%d-r%d-p%d"
    :buffer-size 15000 

	:tries 3	
	})


(map-reduce konfiguracja)


(exit))
