(ns edn2csv.core
  (require [clojure.core.reducers :as r]
           [clojure.edn :as edn]
           [clojure.java.io :as io]
           [clojure.pprint :as pp]
           [iota]
           [me.raynes.fs :as fs])
  (:gen-class))

(defn new-uuid []
    (str (java.util.UUID/randomUUID)))

; The header line for the Individuals CSV file
(def individuals-header-line "UUID:ID(Individual),Generation:int,Location:int,:LABEL")

; Ignores (i.e., returns nil) any EDN entries that don't have the
; 'clojure/individual tag.
(defn individual-reader
    [t v]
    (when (= t 'clojush/individual) v))

; I got this from http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
; It prints in a way that avoids weird interleaving of lines and items.
; In several ways it would be better to use a CSV library like
; clojure.data.csv, but that won't (as written) avoid the interleaving
; problems, so I'm sticking with this approach for now.
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

(defn edn->csv-sequential [edn-file individuals-csv-file semantics-csv-file errors-csv-file parent-of-csv-file individual-semantics-csv-file semantics-error-csv-file]
  (with-open [individuals-out-file (io/writer individuals-csv-file)]
    (safe-println individuals-out-file individuals-header-line)
    (->>
      (line-seq (io/reader edn-file))
      ; Skip the first line because it's not an individual
      (drop 1)
      (map (partial edn/read-string {:default individual-reader}))
      (map (partial print-individual-to-csv individuals-out-file))
      (reduce +)
      )))

(defn edn->csv-pmap [edn-file individuals-csv-file semantics-csv-file errors-csv-file parent-of-csv-file individual-semantics-csv-file semantics-error-csv-file]
  (with-open [individuals-out-file (io/writer individuals-csv-file)]
    (safe-println individuals-out-file individuals-header-line)
    (->>
      (line-seq (io/reader edn-file))
      ; Skip the first line because it's not an individual
      (drop 1)
      (pmap (fn [line]
        (print-individual-to-csv individuals-out-file (edn/read-string {:default individual-reader} line))
        1))
      count
      )))

(defn edn->csv-reducers [edn-file individuals-csv-file semantics-csv-file errors-csv-file parent-of-csv-file individual-semantics-csv-file semantics-error-csv-file]
  (with-open [individuals-out-file (io/writer individuals-csv-file)]
    (safe-println individuals-out-file individuals-header-line)
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      ; This eliminates empty (nil) lines, which result whenever
      ; a line isn't a 'clojush/individual. That only happens on
      ; the first line, which is a 'clojush/run, but we still need
      ; to catch it. We could do that with `r/drop`, but that
      ; totally kills the parallelism. :-(
      (r/filter identity)
      (r/map (partial print-individual-to-csv individuals-out-file))
      (r/fold +)
      )))

(defn build-csv-filename
  [edn-filename strategy type]
  (str (fs/parent edn-filename)
       "/"
       (fs/base-name edn-filename ".edn")
       (if strategy
         (str "_" strategy)
         "_sequential")
       (str "_" type ".csv")))

(defn -main
  [edn-filename & [strategy]]
  (let [individual-csv-file (build-csv-filename edn-filename strategy "Individuals")
        semantics-csv-file (build-csv-filename edn-filename strategy "Semantics")
        errors-csv-file (build-csv-filename edn-filename strategy "Errors")
        parent-of-csv-file (build-csv-filename edn-filename strategy "ParentOf_edges")
        individual-semantics-csv-file (build-csv-filename edn-filename strategy "Individual_Semantics_edges")
        semantics-error-csv-file (build-csv-filename edn-filename strategy "Semantics_Error_edges")]
    (time
      (condp = strategy
        "sequential" (edn->csv-sequential edn-filename individual-csv-file semantics-csv-file errors-csv-file parent-of-csv-file individual-semantics-csv-file semantics-error-csv-file)
        "pmap" (edn->csv-pmap edn-filename individual-csv-file semantics-csv-file errors-csv-file parent-of-csv-file individual-semantics-csv-file semantics-error-csv-file)
        "reducers" (edn->csv-reducers edn-filename individual-csv-file semantics-csv-file errors-csv-file parent-of-csv-file individual-semantics-csv-file semantics-error-csv-file)
        (edn->csv-sequential edn-filename individual-csv-file semantics-csv-file errors-csv-file parent-of-csv-file individual-semantics-csv-file semantics-error-csv-file))))
  ; Necessary to get threads spun up by `pmap` to shutdown so you get
  ; your prompt back right away when using `lein run`.
  (shutdown-agents))
