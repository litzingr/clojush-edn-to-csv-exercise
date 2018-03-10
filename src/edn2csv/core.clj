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
(def semantics-header-line "UUID:ID(Semantics),TotalError:int,:LABEL")
(def errors-header-line "UUID:ID(Error),ErrorValue:int,Location:int,:LABEL")
(def parent-of-header-line ":START_ID(Individual),GeneticOperator,:END_ID(Individual),:TYPE")
(def individual-semantics-header-line ":START_ID(Individual),:END_ID(Semantics),:TYPE")
(def semantics-error-header-line "START_ID(Semantics),:END_ID(Error),:TYPE")

; Ignores (i.e., returns nil) any EDN entries that don't have the
; 'clojure/individual tag.
(defn individual-reader
    [t v]
    (when (= t 'clojush/individual) v))


(def atomic-errors (atom {})) ;;I guess we will just use this for our atomic holder

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
  [individual-csv-file parent-of-csv-file {:keys [uuid, generation, location, parent-uuids, genetic-operators, total-error, errors]}]
  (safe-println individual-csv-file uuid generation location "Individual")
  (doseq [parent parent-uuids] (safe-println parent-of-csv-file parent genetic-operators uuid "PARENT_OF")
  ; I don't like that this is how synchronisity works... Got help with this part...
  (swap! atomic-errors (partial merge-with into) {[total-error errors] [uuid]})
  )
  1)

(defn print-semantics-to-csv
  [semantics-csv-file errors-csv-file individual-semantics-csv-file semantics-error-csv-file [[total-error errors] uuids]]
  (let [new-semantics-uuid (new-uuid)
        error-uid (new-uuid)]
      (safe-println semantics-error-csv-file new-semantics-uuid error-uid "HAS_ERROR")
      (safe-println semantics-csv-file new-semantics-uuid total-error "Semantics")
      (doseq [[idx itm] (map-indexed (fn [idx itm] [idx itm]) errors)] (safe-println errors-csv-file error-uid itm idx "Error"))
      (doseq [old-uuid uuids] (safe-println individual-semantics-csv-file old-uuid new-semantics-uuid "HAS_SEMANTICS"))

  ))

(defn build-csv-filename
  [edn-filename strategy type]
  (str (fs/parent edn-filename)
      "/"
      (fs/base-name edn-filename ".edn")
      (if strategy
      (str "_" strategy)
      "_sequential")
      (str "_" type ".csv")))

(defn edn->csv-reducers [edn-file]
  (with-open [individuals-csv-file (io/writer (build-csv-filename edn-file "reducers" "Individual"))
              semantics-csv-file (io/writer (build-csv-filename edn-file "reducers" "Semantics"))
              errors-csv-file (io/writer (build-csv-filename edn-file "reducers" "Errors"))
              parent-of-csv-file (io/writer (build-csv-filename edn-file "reducers" "ParentOf_edges"))
              individual-semantics-csv-file (io/writer (build-csv-filename edn-file "reducers" "Individual_Semantics_edges"))
              semantics-error-csv-file (io/writer (build-csv-filename edn-file "reducers" "Semantics_Error_edges"))]
    (safe-println individuals-csv-file individuals-header-line)
    (safe-println semantics-csv-file semantics-header-line)
    (safe-println errors-csv-file errors-header-line)
    (safe-println parent-of-csv-file parent-of-header-line)
    (safe-println individual-semantics-csv-file individual-semantics-header-line)
    (safe-println semantics-error-csv-file semantics-error-header-line)
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      (r/filter identity)
      (r/map (partial print-individual-to-csv individuals-csv-file parent-of-csv-file))
      ; (r/map (partial print-semantics-to-csv semantics-csv-file errors-csv-file individual-semantics-csv-file semantics-error-csv-file))
      (r/fold +))
      ; I couldn't figure out how to get this to use reducers...
      (doall (pmap (partial print-semantics-to-csv semantics-csv-file errors-csv-file individual-semantics-csv-file semantics-error-csv-file) @atomic-errors))
      ))

;; I wanted to do all the strategies but I didn't end up having time to do so.


(defn -main
  [edn-filename & [strategy]]
    (time
        (edn->csv-reducers edn-filename)
        )
  ; Necessary to get threads spun up by `pmap` to shutdown so you get
  ; your prompt back right away when using `lein run`.
  (shutdown-agents))
