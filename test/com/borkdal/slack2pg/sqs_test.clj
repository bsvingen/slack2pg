(ns com.borkdal.slack2pg.sqs_test
  (:require [midje.sweet :refer :all]
            [midje.util :refer [testable-privates]]
            [com.borkdal.slack2pg.sqs :refer :all]))

(testable-privates com.borkdal.slack2pg.sqs
                   parse-messages-for-db-xf)

(fact "body-map"
  (transduce (body-map inc) conj [{:dummy :x :body 0}
                                  {:dummier :y :body 1}
                                  {:body 2}])
  => [{:dummy :x :body 1}
      {:dummier :y :body 2}
      {:body 3}])

(facts "parsing messages from SQS"
  (let [parsed-messages (transduce (parse-messages-for-db-xf :batch-size 4)
                                   conj
                                   (clojure.edn/read-string
                                    (slurp "test/resources/sqs-test-data.edn")))]
    (fact "three batches"
      (count parsed-messages) => 3)
    (fact "correct number of message in the batches"
      (map count parsed-messages) => (just 4 4 2))
    (fact "correct number of messages for user 1"
      (count (filter #(= "user_1" (% :user_name))
                     (map :body (flatten parsed-messages))))
      => 7)
    (fact "random message"
      (:text (:body (nth (flatten parsed-messages) 7)))
      => "The eight line.")))

