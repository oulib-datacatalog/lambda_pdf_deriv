Chalice AWS Lambda functions for OU Libraries Derivative and PDF Generation
===

## Requirements

* Python 3.9+
* AWS Chalice

## Installation

1. Ensure access to AWS and proper permissions.
1. Create 2 AWS SQS queues. The functions deployed by this repo will need the following Action roles to be able to access the queues:
    * sqs:DeleteMessage
    * sqs:GetQueueUrl
    * sqs:ReceiveMessage
    * sqs:SendMessage
1. Modify the values of the following in app.py to match the names of the added queues and the S3 bucket containing bagged files:
    * SQS_QUEUE_DERIV
    * SQS_QUEUE_PDF
    * S3_BUCKET
1. Adjust `Default visibility timeout` of SQS queues (default is 30 seconds) to match `lambda_timeout` (default is 60 seconds)
1. Adjust `lambda_memory_size` in Chalice's config.json to your needs before deploying.
1. To deploy run `chalice deploy`

## TODO
* Create deployment script to setup AWS service dependencies and roles
* Store results in a database