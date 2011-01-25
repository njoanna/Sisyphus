(ns naszko.asia.zadania
	"Najpopularniejsze funkcje"
	(:use (naszko.asia commons stats)))




(defncode chunker-rozdziel-pliki
  "Wynikiem jest sekwencja plikow lub katalogow pasujacych do wzorca,
   stad kady mapper bedzie przetwarzal po jednym pliku"
  []
    (let [ [path pattern] (:chunker-params *cfg*) p (re-pattern pattern)]
        (map (memfn getPath) (filter #(->> % str (re-matches p))
            (file-seq (java.io.File. path))))))




(defncode rozdzielenie-modulo
  "funkcja dzielaca wynik z mapera pomiedzy reducerow,
	jest to modulo liczba reducerow z hasha klucza"
  [reducers-num [key value] ]
    (mod (hash key) reducers-num))




(defncode sortowanie-po-kluczu
  "posortuj wedlug klucza"
  [para1 para2]
    (. (first para1) compareTo (first para2)))


(defncode sortowanie-po-kluczu-bez-wielkosci-liter
  "posortuj wedlug klucza ignorujac wielkosc liter, stad 
	zakladamy, ze kluczem sa napisy"
  [para1 para2]
    (. (first para1) compareToIgnoreCase (first para2)))





(defncode inputer-odczyt-calego-pliku
  "sposob odczytania zadanej pracy, trzeba zwrocic sekwencje
    w postaci klucz wartosc dla operacji pierwszego przeksztalcenia"
  [nazwa]
	(add-counter :in (file-size nazwa))
    [ [nazwa (slurp nazwa) ]] )


(defncode inputer-odczyt-po-linii
  "odczytuje plik i zwraca sekwencje gdzie wartosciami sa kolejne linie"
  [nazwa]
	(add-counter :in (file-size nazwa))
        (map vector (repeat nazwa) (map vector (drop 1 (range)) (read-lines nazwa))))




(defncode outputer-zapis-wartosci-sekwencji-par
  "jako wyniku spowdziewa sie sekwencji par, zapisuje
   do pliku klucz spacja wartosc"
  [wynik]
	(let [outname (format (:outputer-params *cfg*) *worker-id*)]
     (with-out-writer outname
        (doseq [ [k v] wynik ] (println k v))) 
		(add-counter :out  (file-size outname))
		outname))


(defncode outputer-zapis-wartosci-sekwencji
  "jako wyniku spodziewa sie sekwencji jakis wartosci,
	zapisuje te wartosci do pliku w kolejnych liniach"
  [wynik]
	(let [outname (format (:outputer-params *cfg*) *worker-id*)]
    (with-out-writer outname
      (dorun (map println wynik))
		(add-counter :out (file-size outname))
		outname)))





(defncode mapowanie-wyliczanka
  "przeksztalc na sekwencje par: slowo, wartosc jeden"
  [[nazwa-pliku zawartosc-pliku]]
    (for [slowo (re-seq #"[^\W]+" zawartosc-pliku)] 
      [(.toLowerCase slowo) 1]))



(defncode redukcja-podsumowanie
  "przeksztalc na sekwencje par: slowo, suma jego wartosci"
  [[slowo sekwencje-wartosci]]
    [ [slowo (reduce + sekwencje-wartosci)] ])


(defncode redukcja-identycznosc
  "przeksztalc pare: slowo, sekwencja wartosci ; na sekwencje par: slowo, wartosc
   to jest identycznosc wzgledem operacji map, czyli to co map wyplul, 
   znajdzie sie na wyjsciu map/reduce"
  [ [ klucz sekwencja-wartosci] ] (for[ wartosc sekwencja-wartosci ] [ klucz wartosc] ) )


(defncode redukcja-klucze
  "koncowe operacje na wynikach czastkowych z etapu przeksztalcania"
  [[klucz sekwencja-wartosci]] [ klucz ])


(defncode redukcja-unikaty
  "przeksztalc pare: slowo, sekwencja wartosci ; na sekwencje par: slowo, unikatowa wartosc"
  [[klucz sekwencja-jego-wartosci]] 
	 (for [x  (distinct sekwencja-jego-wartosci) ]
		[ klucz x ]))





(defncode redukcja-unikaty-wartosci
	"przeksztalc pare: slowo, sekwencja wartosci ;
	w sekwencje par: slowo, sekwencja unikatowych wartosci"
	[ [klucz sekwencja-wartosci] ]
			[ [ klucz (distinct sekwencja-wartosci)] ])





