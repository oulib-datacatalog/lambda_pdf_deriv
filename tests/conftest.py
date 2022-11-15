import os
import pytest
import boto3
from moto import mock_s3, mock_sqs


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def default_env():
    os.environ["SQS_QUEUE_DERIV"] = "test-queue"
    os.environ["SQS_QUEUE_PDF"] = "test-queue"
    os.environ["S3_BUCKET"] = "test-bucket"


@pytest.fixture
def bucket_name(default_env):
    return os.environ["S3_BUCKET"]


@pytest.fixture
def s3_client(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn


@pytest.fixture
def s3_test(s3_client, bucket_name):
    s3_client.create_bucket(Bucket=bucket_name)
    yield


@pytest.fixture
def sqs_resource(aws_credentials):
    with mock_sqs():
        conn = boto3.resource("sqs", region_name="us-east-1")
        yield conn


@pytest.fixture
def sqs_client(aws_credentials):
    with mock_sqs():
        conn = boto3.client("sqs", region_name="us-east-1")
        yield conn


@pytest.fixture
def sqs_test(sqs_resource):
    queue = sqs_resource.create_queue(
        QueueName = "test-queue",
        Attributes = {
            "VisibilityTimeout": "60"
        }
    )
    yield queue