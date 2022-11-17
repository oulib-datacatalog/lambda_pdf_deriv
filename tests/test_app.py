import os
import boto3
import botocore
import pytest
from io import BytesIO
from functools import cache
from chalice.test import Client
from chalice.app import NotFoundError, BadRequestError
from PIL import Image, ImageColor

from app import app, get_s3_client, get_s3_paginator, get_sqs, get_deriv_queue, get_pdf_queue, _is_file_too_large, _filter_keep, _images, \
    _find_source_bag, _s3_byte_stream, _object_size, _generate_pdf
from app import DEFAULT_IMAGE_EXTENSIONS, DEFAULT_IMAGE_SCALE


def test_default_regions(aws_credentials, s3_client):
    session = boto3.Session()
    assert session.region_name == "us-east-1"


def test_s3_client(s3_client, s3_test):
    bucket = os.environ["S3_BUCKET"]
    client = boto3.client("s3", region_name="us-east-1")
    assert [bucket["Name"] for bucket in client.list_buckets()["Buckets"]] == [bucket]


def test_caching():

    class ExampleClass():
        pass
    
    @cache
    def cached_result():
        return ExampleClass()
    
    def noncached_result():
        return ExampleClass()
    
    c1 = cached_result()
    c2 = cached_result()
    n1 = noncached_result()
    n2 = noncached_result()

    assert id(c1) == id(c2)  # these are the same object instance
    assert id(n1) != id(n2)  # these point to different instances


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


def test__is_file_too_large():
    assert _is_file_too_large(file_sizes=1024, max_size=1024, buffer_ratio=0.3) == True
    assert _is_file_too_large(file_sizes=(1024), max_size=1024, buffer_ratio=0.3) == True
    assert _is_file_too_large(file_sizes=716, max_size=1024, buffer_ratio=0.3) == False
    assert _is_file_too_large(file_sizes=717, max_size=1024, buffer_ratio=0.3) == True
    assert _is_file_too_large(file_sizes=716.8, max_size=1024, buffer_ratio=0.3) == False
    assert _is_file_too_large(file_sizes=716.9, max_size=1024, buffer_ratio=0.3) == True


def test__filter_keep():
    for ext in DEFAULT_IMAGE_EXTENSIONS:
        assert _filter_keep(f"data/test.{ext}") == True
        assert _filter_keep(f"data/test_orig.{ext}") == False
        assert _filter_keep(f"data/.test.{ext}") == False
        assert _filter_keep(f"data/.test.{ext}.bak") == False

    assert "gif" not in DEFAULT_IMAGE_EXTENSIONS
    assert _filter_keep("data/test.gif") == False


def test__images(s3_client, s3_test, bucket_name):
    prefix = "source/test_bag_2022"
    body = "test"
    count = 10
    for index in range(count):
        s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/data/image{index:03}.tif", Body=body)
        s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/data/image{index:03}_orig.tif", Body=body)
        s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/data/image{index:03}.CR2", Body=body)
    
    assert list(_images(prefix=prefix)) == [{"file": f"{prefix}/data/image{index:03}.tif", "size": len(body)} for index in range(count)]
    assert list(_images(prefix=prefix, extensions=("does_not_exist_in_list"))) == []


def test__find_source_bag(s3_client, s3_test, bucket_name):
    prefix = "source/test_bag_2022"
    s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/bagit.txt", Body="test")
    assert _find_source_bag("test_bag_2022") == {"location": prefix}

    with pytest.raises(NotFoundError):
        _find_source_bag("does_not_exist")
    

def test__s3_byte_stream(s3_client, s3_test, bucket_name):
    prefix = "source/test_bag_2022"
    body = b"test"
    s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/bagit.txt", Body=body)
    assert _s3_byte_stream(bucket=bucket_name, key=f"{prefix}/bagit.txt").read() == body

    with pytest.raises(NotFoundError):
        _s3_byte_stream(bucket=bucket_name, key="does_not_exist").read()


def test__object_size(s3_client, s3_test, bucket_name):
    prefix = "source/test_bag_2022"
    body = b"test"
    s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/bagit.txt", Body=body)
    assert _object_size(bucket=bucket_name, key=f"{prefix}/bagit.txt") == len(body)

    with pytest.raises(NotFoundError):
        _object_size(bucket=bucket_name, key="does_not_exist")


def test__generate_pdf(s3_client, s3_test, bucket_name):
    prefix = f"derivative/test_bag_2022/{DEFAULT_IMAGE_SCALE}"
    count = 10
    with BytesIO() as output:
        Image.new( mode = "RGB", size = (300, 400), color = ImageColor.getrgb("#841617") ).save(output, format="JPEG")
        for index in range(count):
            output.seek(0)
            s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/data/image{index:03}.jpg", Body=output)
    assert _generate_pdf("test_bag_2022") == {"message": "success"}
    assert _generate_pdf("test_bag_2022") == {"message": "PDF already exists"}
    assert _generate_pdf("does_not_exist") == {"message": "missing derivative to generate PDF"}


def test__generate_pdf_invalid_image(s3_client, s3_test, bucket_name):
    prefix = f"derivative/test_bag_2022/{DEFAULT_IMAGE_SCALE}"
    s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/data/image001.jpg", Body=b"invalid image data")
    with pytest.raises(BadRequestError):
        _generate_pdf("test_bag_2022") == {"message": "success"}