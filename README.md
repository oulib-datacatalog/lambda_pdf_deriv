Chalice AWS Lambda functions for OU Libraries Derivative and PDF Generation
===

## Requirements

* Python 3.9+
* AWS Chalice

## Installation

1. Ensure access to AWS and proper permissions.
1. Create SQS queues and configure app.py to use these queues. The functions need the following Action roles to be able to access the queues:
    * sqs:DeleteMessage
    * sqs:GetQueueUrl
    * sqs:ReceiveMessage
    * sqs:SendMessage
1. To deploy run `chalice deploy`
