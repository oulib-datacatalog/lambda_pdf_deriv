import io
import logging

from json import dumps, loads
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
from botocore.exceptions import ClientError
from chalice import Chalice, NotFoundError
from PIL import Image

DEFAULT_IMAGE_SCALE = 0.4  # of original size
DEFAULT_IMAGE_EXTENSIONS = ("jpg", "tif", "tiff", "png")

SQS_QUEUE_DERIV = "infx_deriv_gen"
SQS_QUEUE_PDF = "infx_pdf_gen"


app = Chalice(app_name='lambda_pdf_deriv')
app.debug = True
app.log.setLevel(logging.DEBUG)

s3_client = boto3.client('s3')
s3_paginator = s3_client.get_paginator('list_objects_v2')
sqs = boto3.resource('sqs')
s3_bucket = 'tdp-bagit'

deriv_queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_DERIV)
pdf_queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_PDF)


def _images(prefix, extensions=DEFAULT_IMAGE_EXTENSIONS, ignore_orig=True):
    """ yield images in S3 with filtering """
    for page in s3_paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        try:
            contents = page['Contents']
        except KeyError as e:
            app.log.error(f"Failed to access: {e}")
            raise NotFoundError("Could not find bag matching request!")
        for obj in contents:
            try:
                file = obj['Key']
            except KeyError as e:
                app.log.error(f"Failed to access: {e}")
                raise NotFoundError("Could not find images matching request!")
            if file.endswith(extensions):
                if not (ignore_orig and "orig" in Path(file).name.lower()):
                    yield file


@app.route('/images/source/{bag}')
def images_source(bag):
    """ API endpoint to list available source images """
    return list(_images(f'source/{bag}/data/'))


@app.route('/images/derivatives/{bag}/{scale}')
def images_derivative(bag, scale=DEFAULT_IMAGE_SCALE):
    """ API endpoint to list available images at specified scale """
    return list(_images(f'derivative/{bag}/{scale}/', extensions=DEFAULT_IMAGE_EXTENSIONS + ("pdf",)))


@app.route('/images/derivatives/{bag}')
def available_derivatives(bag):
    """ API endpoint to list available derivative scales """
    # FIXME: this will get the first 1000 items matched against prefix and may miss some results
    return list(
        set(
            [
                item['Key'].split("/")[2]
                for item in s3_client.list_objects(Bucket=s3_bucket, Prefix=f'derivative/{bag}/')['Contents']
            ]
        )
    )


def _s3_byte_stream(bucket, key):
    """ return an S3 object's data as a BytesIO stream """
    return io.BytesIO(s3_client.get_object(Bucket=bucket, Key=key)['Body'].read())


# TODO: capture metadata to be included in generated PDF
# https://pillow.readthedocs.io/en/stable/handbook/image-file-formats.html#pdf
def _generate_pdf(bag):
    """ Generates PDF from default derivative images """
    destination = f"derivative/{bag}/pdf/{bag}.pdf"

    try:  # Test for existing pdf
        s3_client.head_object(Bucket=s3_bucket, Key=destination)
        app.log.info(f"PDF already exists: {destination}")
        return {"message": "PDF already exists"}
    except ClientError:
        pass  # does not exist - continue
    
    try:  # Test for existing derivatives
        image_paths = images_derivative(bag, scale=DEFAULT_IMAGE_SCALE)
    except NotFoundError:
        app.log.error("Missing derivative - failed to generate PDF")
        return {"message": "missing derivative to generate PDF"}

    # get contents of first image to bootstrap PDF generation
    image_path = image_paths.pop(0) 
    pdf = Image.open(_s3_byte_stream(bucket=s3_bucket, key=image_path))
    
    # save generated PDF back to S3
    pdf_file = io.BytesIO()
    pdf.save(
        pdf_file,
        format="PDF",
        save_all=True,
        append_images=(
            Image.open(_s3_byte_stream(bucket=s3_bucket, key=image_path))
            for image_path in image_paths
        )
    )
    pdf_file.flush()
    pdf_file.seek(0)
    s3_client.put_object(Bucket=s3_bucket, Key=destination, Body=pdf_file)    


@app.on_sqs_message(queue=SQS_QUEUE_PDF, batch_size=1)
def pdf_generator(event):
    """ Watch for messages to generate PDF """
    for record in event:
        record_body = record.body
        app.log.debug(record_body)
        _generate_pdf(record_body)


@app.route('/pdf/{bag}')
def generate_pdf(bag):
    """ API endpoint for requesting PDF generation """
    app.log.debug(f"Using queue: {pdf_queue}")
    app.log.info(f"Processing {bag}")
    resp = pdf_queue.send_message(
        MessageBody=bag
    )
    app.log.debug(resp)
    return {"message": "submitted for processing"}
        

@app.route('/resize/{scale}/{bag}/{image_path}')
def resize_individual(scale, bag, image_path):
    """ API endpoint to resize specific image """
    deriv_image_path = Path(image_path).with_suffix(".jpg")
    destination = f"derivative/{bag}/{scale}/{deriv_image_path}"
    
    try:  # Test for existing derivative
        s3_client.head_object(Bucket=s3_bucket, Key=destination)
        app.log.info(f"Derivative already exists: {destination}")
        return {"message": "image already exists"}
    except ClientError:
        pass  # does not exist - continue
    
    image_data = s3_client.get_object(Bucket=s3_bucket, Key=f"source/{bag}/data/{image_path}")
    source_image = Image.open(io.BytesIO(image_data['Body'].read()))

    size = (x * float(scale) for x in source_image.size)
    source_image.thumbnail(size, Image.ANTIALIAS)

    image_file = io.BytesIO()
    source_image.save(image_file, format="JPEG")
    image_file.flush()
    image_file.seek(0)
    
    s3_client.put_object(Bucket=s3_bucket, Key=destination, Body=image_file)
    app.log.info(f"Created S3 object: {destination}")
    return {"message": "created resized image"}


@app.on_sqs_message(queue=SQS_QUEUE_DERIV, batch_size=1)
def deriv_generator(event):
    """ watch for messages to generate an image derivative """
    for record in event:
        record_body = record.body
        app.log.debug(record_body)
        scale, bag, image = loads(record_body)
        resize_individual(scale, bag, image)


@app.route('/resize/{scale}/{bag}')
def resize(scale, bag):
    """ API endpoint to resize images for specified bag """
    app.log.debug(f"Using queue: {deriv_queue}")
    app.log.info(f"Processing {bag}")
    for full_s3_key in images_source(bag):
        image = full_s3_key.split("/")[-1]
        resp = deriv_queue.send_message(
            MessageBody=dumps((scale, bag, image))
        )
        app.log.debug(resp)
    return {"message": "submitted for processing"}


@app.route('/resize/default/{bag}')
def resize_default(bag):
    """ API endpoint to resize images by default scale for specified bag """
    return resize(scale=DEFAULT_IMAGE_SCALE, bag=bag)
