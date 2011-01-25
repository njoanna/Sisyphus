(ns naszko.asia.zadanie2
	"Looking up for a sequence of chars in a set of text documents.
	The result is a set of pairs of: name of a file, line where the sequence has occured.

	Odnajdywanie ciagu w zbiorze plików tekstowych.
  	Wynikiem jest zbiór par złożonych z nazwy pliku i wiersza"
	(:use (naszko.asia commons master zadania)))

(with-master

(defncode mapowanie-slowo-w-pliku
  "dla zadanego ciagu/slowa, przeksztalc na sekwencje par: ten ciag/slowo, nazwa pliku"
  [[nazwa-pliku [numer-linii linia-z-pliku]] ]
      (let [word (:mapper-params *cfg*)]
         (when (re-matches (re-pat ".*?" word ".*?")
            (.toLowerCase linia-z-pliku))
                [[nazwa-pliku [numer-linii linia-z-pliku]]])))


(def konfiguracja {

		

    :chunker-params [ "./teksty" ".+\\.txt" ]
    :chunker chunker-rozdziel-pliki


    :inputer inputer-odczyt-po-linii

	:outputer-params "./wynik/zad2-wynik_%d.txt"
    :outputer outputer-zapis-wartosci-sekwencji-par

    :sorter sortowanie-po-kluczu

	:mapper-params "pretium"
    :mapper  mapowanie-slowo-w-pliku
                   
    :reducer redukcja-identycznosc
    :partitioner rozdzielenie-modulo

    :reducers-num 2
    :intermediate "./temp/z2-iter-m%d-r%d-p%d"
    :buffer-size 15000 

	:tries 3
	})


(map-reduce konfiguracja)

(exit))
