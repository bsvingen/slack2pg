(ns com.borkdal.slack2pg.cloudformation-test
  (:require [midje.sweet :refer :all]
            [midje.util :refer [testable-privates]]
            [com.borkdal.slack2pg.cloudformation :refer :all]))

(testable-privates com.borkdal.slack2pg.cloudformation
                   make-template)

(facts "CloudFormation template"
  (let [template (make-template)]
    (fact "should be map"
      template => map?)
    (fact "should have correct keys"
      (keys template) => (contains "Resources" "Description" "Outputs"
                                   :in-any-order :gaps-ok))))

