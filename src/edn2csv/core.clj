(ns edn2csv.core
  (require [clojure.core.reducers :as r]
           [clojure.edn :as edn]
           [clojure.java.io :as io]
           [clojure.pprint :as pp]
           [iota])
  (:gen-class))

; Ignores (i.e., returns nil) any EDN entries that don't have the
; 'clojure/individual tag.
(defn individual-reader
    [t v]
    (when (= t 'clojush/individual) v))

; I got this from http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
; It prints in a way that avoids weird interleaving of lines and items.
; We may not need this when we write with the csv library.
(defn safe-println [output-stream & more]
  (.write output-stream (str (clojure.string/join "," more) "\n")))

; This prints out the relevant fields to the CSV filter
; and then returns 1 so we can count up how many individuals we processed.
; (The counting isn't strictly necessary, but it gives us something to
; fold together after we map this across the individuals; otherwise we'd
; just end up with a big list of nil's.)
(defn print-individual-to-csv
  [csv-file line]
  (as-> line $
    (map $ [:uuid :generation :location])
    (concat $ ["Individual"])
    (apply safe-println csv-file $))
  1)

(defn edn->csv-sequential [edn-file csv-file]
  (with-open [out-file (io/writer csv-file)]
    (->>
      (line-seq (io/reader edn-file))
      ; Skip the first line because it's not an individual
      (drop 1)
      (map (partial edn/read-string {:default individual-reader}))
      (map (partial print-individual-to-csv out-file))
      (reduce +)
      )))

(defn edn->csv-pmap [edn-file csv-file]
  (with-open [out-file (io/writer csv-file)]
    (->>
      (line-seq (io/reader edn-file))
      ; Skip the first line because it's not an individual
      (drop 1)
      (pmap (fn [line]
        (print-individual-to-csv out-file (edn/read-string {:default individual-reader} line))
        1))
      count
      )))

(defn edn->csv-iota [edn-file csv-file]
  (with-open [out-file (io/writer csv-file)]
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      ; This eliminates empty (nil) lines, which result whenever
      ; a line isn't a 'clojush/individual. That only happens on
      ; the first line, which is a 'clojush/run, but we still need
      ; to catch it. We could do that with `r/drop`, but that
      ; totally kills the parallelism. :-(
      (r/filter identity)
      (r/map (partial print-individual-to-csv out-file))
      (r/fold +)
      )))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
