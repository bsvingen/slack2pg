(ns com.borkdal.slack2pg.common_test
  (:require [midje.sweet :refer :all]
            [com.borkdal.slack2pg.common :refer :all]
            [clj-time.coerce :refer [to-date-time]]))

(fact "keywordize-keys"
  (keywordize-keys {"userId" 7
                    "channel_id" 11})
  => {:user_id 7
      :channel_id 11})

(fact "parse-timestamp-value"
  (to-date-time
   (:timestamp
    (parse-timestamp-value {:timestamp "1437186020.008831"})))
  => (to-date-time "2015-07-18T02:20:20.009Z"))

(facts "flushable-partition-all"
  (fact "straight-forward"
    (transduce (flushable-partition-all 3) conj [1 2 :flush 3 4 5 6 7 :flush 8 9])
    => [[1 2] [3 4 5] [6 7] [8 9]])
  (fact "edges"
    (transduce (flushable-partition-all 3) conj [:flush 1 2 :flush 3 4 5 6 7 :flush 8 9 :flush])
    => [[1 2] [3 4 5] [6 7] [8 9]]))

