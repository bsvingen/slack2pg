(ns com.borkdal.slack2pg.export_test
  (:require [midje.sweet :refer :all]
            [midje.util :refer [testable-privates]]
            [com.borkdal.slack2pg.export :refer :all]))

(testable-privates com.borkdal.slack2pg.export
                   get-zip-messages)

(facts "export ZIP parsing"
  (let [parsed-messages (get-zip-messages "test/resources/test-export.zip"
                                          "a_domain_id"
                                          "a_domain")]
    (fact "single batch"
      (count parsed-messages) => 1)
    (fact "number of messages"
      (count (first parsed-messages)) => 7)
    (fact "messages"
      (map :text (first parsed-messages))
      => (just "<@user_1|user_1> has joined the channel"
               "Message one."
               "Message two."
               "Message three."
               "Message four."
               "Message five."
               "Another message."
               :in-any-order))))

