import os
import pytest
import boto3
from moto import mock_aws

@pytest.fixture
def aws_credentials():
    """ assert mock environment set from pytest.ini """
    assert os.environ["AWS_ACCESS_KEY_ID"] == "TESTING"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "TESTING"
    assert os.environ["AWS_SECURITY_TOKEN"] == "TESTING"
    assert os.environ["AWS_SESSION_TOKEN"] == "TESTING"
    assert os.environ["AWS_DEFAULT_REGION"] == "us-east-1"


@pytest.fixture
def default_env():
    assert os.environ["SQS_QUEUE_DERIV"] == "TEST-DERIV-QUEUE"
    assert os.environ["SQS_QUEUE_PDF"] == "TEST-PDF-QUEUE"
    assert os.environ["S3_BUCKET"] == "TEST-BUCKET"


@pytest.fixture
def bucket_name(default_env):
    return os.environ["S3_BUCKET"]


@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def s3_test(s3_client, bucket_name):
    s3_client.create_bucket(Bucket=bucket_name)
    yield


@pytest.fixture
def sqs_resource(aws_credentials):
    with mock_aws():
        yield boto3.resource("sqs", region_name="us-east-1")


@pytest.fixture
def sqs_client(aws_credentials):
    with mock_aws():
        yield boto3.client("sqs", region_name="us-east-1")


@pytest.fixture
def deriv_queue(default_env):
    return os.environ["SQS_QUEUE_DERIV"]


@pytest.fixture
def sqs_test_deriv(sqs_resource, deriv_queue):
    yield sqs_resource.create_queue(
        QueueName = deriv_queue,
        Attributes = {
            "VisibilityTimeout": "60"
        }
    )


@pytest.fixture
def pdf_queue(default_env):
    return os.environ["SQS_QUEUE_PDF"]


@pytest.fixture
def sqs_test_pdf(sqs_resource, pdf_queue):
    yield sqs_resource.create_queue(
        QueueName = pdf_queue,
        Attributes = {
            "VisibilityTimeout": "60"
        }
    )
