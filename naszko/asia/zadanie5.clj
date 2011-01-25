(ns naszko.asia.zadanie5
	"Creates statistic of used words. 
	The result is a sorted list of pairs: word, numer of occurences.

	Statystyka użytych słów. Wynikiem jest uporządkowana
	lista par: słowo i liczba wystąpień słowa. 
	Uporządkowanie następuje po liczbie wystąpień."
	(:use (naszko.asia commons master zadania)))

(with-master

(map-reduce {

    :chunker-params [ "./teksty" ".+\\.txt" ]
    :chunker chunker-rozdziel-pliki

    
             

    :inputer inputer-odczyt-calego-pliku

	:outputer-params "./temp/zad5-temp-wynik_%d.txt"
    :outputer outputer-zapis-wartosci-sekwencji-par

    :sorter sortowanie-po-kluczu
    :mapper  mapowanie-wyliczanka
    :reducer redukcja-podsumowanie
    :partitioner rozdzielenie-modulo

	:combiner redukcja-podsumowanie

    :reducers-num 3
    :intermediate "./temp/z5a-iter-m%d-r%d-p%d"
    :buffer-size 15000

	:tries 3	
	 })




(defncode mapowanie-parsowanie-klucz-wartosc-jako-liczba
  "zamien sekwencje linii ktore maja postac: slowo, liczba jako napis;
   na sekwencje par: slowo, liczba"
  [[nazwa-pliku [numer-linii zawartosc-linii]]]
      (let [z (.split zawartosc-linii "\\s+")]
        [[(get z 0) (Integer. (get z 1))]]))


(defncode sortowanie-po-wartosc-potem-po-kluczu
  "posortuj wedle wystapien, a dopiero wedlug slowa"
  [[k1 v1] [k2 v2]]
    (let [r (. v2 compareTo v1) ]
      (if (zero? r) 
            (. k1 compareTo k2)
        r)))



(map-reduce {

    :chunker-params [ "./temp" ".+\\.txt" ]
	:chunker chunker-rozdziel-pliki

    


    :inputer inputer-odczyt-po-linii

	:outputer-params "./wynik/zad5-wynik_%d.txt"
    :outputer outputer-zapis-wartosci-sekwencji-par

    :sorter sortowanie-po-wartosc-potem-po-kluczu

    :mapper  mapowanie-parsowanie-klucz-wartosc-jako-liczba

    :reducer redukcja-identycznosc

    :partitioner rozdzielenie-modulo

    :reducers-num 1
    :intermediate "./temp/z5b-iter-m%d-r%d-p%d"
    :buffer-size 15000 

	:tries 3	
	})



(exit))
