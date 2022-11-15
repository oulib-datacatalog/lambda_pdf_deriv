import os
import boto3
from chalice.test import Client
from app import app, get_deriv_queue, _is_file_too_large


def test_default_regions(aws_credentials, s3_client):
    session = boto3.Session()
    assert session.region_name == "us-east-1"


def test_s3_client(s3_client, s3_test):
    bucket = os.environ["S3_BUCKET"]
    client = boto3.client("s3", region_name="us-east-1")
    assert [bucket["Name"] for bucket in client.list_buckets()["Buckets"]] == [bucket]


def test_get_deriv_queue(sqs_resource, sqs_test, default_env):
    assert os.environ["SQS_QUEUE_DERIV"] == "test-queue"
    #assert get_deriv_queue() == None


def test_is_file_too_large():
    assert _is_file_too_large(file_sizes=1024, max_size=1024, buffer_ratio=0.3) == True
    assert _is_file_too_large(file_sizes=(1024), max_size=1024, buffer_ratio=0.3) == True
    assert _is_file_too_large(file_sizes=716, max_size=1024, buffer_ratio=0.3) == False
    assert _is_file_too_large(file_sizes=717, max_size=1024, buffer_ratio=0.3) == True