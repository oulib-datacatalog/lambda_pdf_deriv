import os
import boto3
import botocore
from chalice.test import Client
from app import app, get_s3_client, get_s3_paginator, get_sqs, get_deriv_queue, get_pdf_queue, _is_file_too_large, _filter_keep
from app import DEFAULT_IMAGE_EXTENSIONS


def test_default_regions(aws_credentials, s3_client):
    session = boto3.Session()
    assert session.region_name == "us-east-1"


def test_s3_client(s3_client, s3_test):
    bucket = os.environ["S3_BUCKET"]
    client = boto3.client("s3", region_name="us-east-1")
    assert [bucket["Name"] for bucket in client.list_buckets()["Buckets"]] == [bucket]


def test_get_s3_client_reuses_object_instance(s3_client):
    s3_1 = get_s3_client()
    s3_2 = get_s3_client()
    assert id(s3_1) == id(s3_2)  # these are the same object instance
    assert id(s3_1) != id(s3_client)  # these point to different instances


def test_get_s3_paginator_reuses_object_instance():
    paginator_1 = get_s3_paginator()
    paginator_2 = get_s3_paginator()
    assert id(paginator_1) == id(paginator_2)  # these are the same object instance


def test_get_sqs_reuses_object_instance():
    sqs_1 = get_sqs()
    sqs_2 = get_sqs()
    assert id(sqs_1) == id(sqs_2)  # these are the same object instance


def test_get_deriv_queue_reuses_object_instance(sqs_test_deriv):
    deriv_queue_1 = get_deriv_queue()
    deriv_queue_2 = get_deriv_queue()
    assert id(deriv_queue_1) == id(deriv_queue_2)  # these are the same object instance


def test_get_pdf_queue_reuses_object_instance(sqs_test_pdf):
    pdf_queue_1 = get_pdf_queue()
    pdf_queue_2 = get_pdf_queue()
    assert id(pdf_queue_1) == id(pdf_queue_2)  # these are the same object instance


def test_is_file_too_large():
    assert _is_file_too_large(file_sizes=1024, max_size=1024, buffer_ratio=0.3) == True
    assert _is_file_too_large(file_sizes=(1024), max_size=1024, buffer_ratio=0.3) == True
    assert _is_file_too_large(file_sizes=716, max_size=1024, buffer_ratio=0.3) == False
    assert _is_file_too_large(file_sizes=717, max_size=1024, buffer_ratio=0.3) == True
    assert _is_file_too_large(file_sizes=716.8, max_size=1024, buffer_ratio=0.3) == False
    assert _is_file_too_large(file_sizes=716.9, max_size=1024, buffer_ratio=0.3) == True


def test_filter_keep():
    for ext in DEFAULT_IMAGE_EXTENSIONS:
        assert _filter_keep(f"data/test.{ext}") == True
        assert _filter_keep(f"data/test_orig.{ext}") == False
        assert _filter_keep(f"data/.test.{ext}") == False
        assert _filter_keep(f"data/.test.{ext}.bak") == False

    assert "gif" not in DEFAULT_IMAGE_EXTENSIONS
    assert _filter_keep("data/test.gif") == False