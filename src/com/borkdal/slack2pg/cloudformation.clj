(ns com.borkdal.slack2pg.cloudformation
  (:refer-clojure :exclude [ref])
  (:require [com.comoyo.condensation
             [template :refer [defresource
                               defoutput
                               ref
                               get-att
                               resources
                               outputs
                               template]]
             [stack :as stack]]
            [com.borkdal.slack2pg.config :as config]
            [clojure.pprint :refer [pprint]]))

(defresource sqs-queue
  {"Type" "AWS::SQS::Queue"
   "Properties" {"MessageRetentionPeriod" "1209600"}})

(defoutput queue-url
  "URL of Slack SQS queue"
  (ref sqs-queue))

(defoutput queue-name
  "Name of Slack SQS queue"
  (get-att sqs-queue "QueueName"))

(defn- sqs-policy
  []
  {"PolicyName" "sqs-policy"
   "PolicyDocument" {"Version" "2012-10-17"
                     "Statement" [{"Effect" "Allow"
                                   "Action" ["sqs:*"]
                                   "Resource" (get-att sqs-queue "Arn")}]}})

(defresource slack-user
  {"Type" "AWS::IAM::User"
   "Properties" {"Policies" [(sqs-policy)]}})

(defresource slack-access-key
  {"Type" "AWS::IAM::AccessKey"
   "Properties" {"UserName" (ref slack-user)}})

(defoutput slack-user-access-key
  "Access key for Slack user"
  (ref slack-access-key))

(defoutput slack-user-secret-key
  "Secret key for Slack user"
  (get-att slack-access-key "SecretAccessKey"))

(defn- make-template
  []
  (template :description "Slack SQS integration setup"
            :resources (resources sqs-queue
                                  slack-user
                                  slack-access-key)
            :outputs (outputs queue-url
                              queue-name
                              slack-user-access-key
                              slack-user-secret-key)))

(defn get-stack-name
  [team]
  (str "slack-sqs-setup-" team))

(defn get-queue-url-for-team
  [team]
  (:queue-url
   (stack/get-outputs
    (get-stack-name team))))

(defn deploy
  [& {:keys [team]
      :or {team (:team config/*config*)}}]
  (let [stack-name (get-stack-name team)]
    (stack/create-or-update-stack :stack-name stack-name
                                  :template (make-template))
    (pprint (stack/get-outputs stack-name))))

