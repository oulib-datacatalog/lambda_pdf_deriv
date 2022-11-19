import io
import traceback
from json import dumps, loads
from os import getenv
from pathlib import Path
from typing import Iterator, BinaryIO
from functools import cache

import boto3
from botocore.exceptions import ClientError
from chalice import Chalice, NotFoundError, BadRequestError
from chalice.app import ConvertToMiddleware
from aws_lambda_powertools import Logger
from aws_lambda_powertools import Tracer

from PIL import Image, UnidentifiedImageError


#########################################################
# Default values

DEFAULT_IMAGE_SCALE = 0.4  # of original size
DEFAULT_IMAGE_EXTENSIONS = ('jpg', 'tif', 'tiff', 'png')

SOURCE_BAG_LOCATIONS = ['source', 'private/external-preservation', 'private/preservation', 'private/private', 'private/shareok']

LAMBDA_MAX_MEMORY_FOR_PDF = 2147483648  # 2GB
LAMBDA_MAX_MEMORY_FOR_DERIV = 1073741824  # 1GB


########################################################
# Configurations from environment

SQS_QUEUE_DERIV = getenv("SQS_QUEUE_DERIV")
SQS_QUEUE_PDF = getenv("SQS_QUEUE_PDF")
S3_BUCKET = getenv("S3_BUCKET")


########################################################
# Chalice application settings

app = Chalice(app_name='lambda_pdf_deriv')
app.debug = True

logger = Logger()  # See .chalice/config.json to change LOG_LEVEL
tracer = Tracer()
app.register_middleware(ConvertToMiddleware(logger.inject_lambda_context))
app.register_middleware(ConvertToMiddleware(tracer.capture_lambda_handler))

########################################################
# AWS bindings

@cache
def get_s3_client():
    return boto3.client('s3')


@cache
def get_s3_paginator():
    s3_client = get_s3_client()
    return s3_client.get_paginator('list_objects_v2')


@cache
def get_sqs():
    return boto3.resource('sqs')


@cache
def get_deriv_queue():
    sqs = get_sqs()
    return sqs.get_queue_by_name(QueueName=SQS_QUEUE_DERIV)


@cache
def get_pdf_queue():
    sqs = get_sqs()
    return sqs.get_queue_by_name(QueueName=SQS_QUEUE_PDF)


########################################################
# Helper functions

def _filter_keep(file: str, extensions: tuple[str, ...] = DEFAULT_IMAGE_EXTENSIONS, ignore_orig: bool = True) -> bool:
    """ filters to apply to a file's name to determine if to keep """
    if not file.endswith(extensions):
        return False
    if ignore_orig and 'orig' in Path(file).name.lower():
        return False
    if Path(file).name.startswith('.'):
        return False
    return True


def _images(prefix: str, extensions: tuple[str, ...] = DEFAULT_IMAGE_EXTENSIONS, ignore_orig: bool = True) -> Iterator[dict]:
    """ yield image paths and file sizes in S3 with applied filtering """
    for page in get_s3_paginator().paginate(Bucket=S3_BUCKET, Prefix=prefix):
        try:
            contents = page['Contents']
        except KeyError as e:
            logger.error(f'Failed to access: {e}')
            raise NotFoundError('Could not find bag matching request!')
        for obj in contents:
            try:
                file = obj['Key']
                size = obj['Size']
            except KeyError as e:
                logger.error(f'Failed to access: {e}')
                raise NotFoundError('Could not find images matching request!')

            if _filter_keep(file, extensions=extensions, ignore_orig=ignore_orig):
                yield {'file': file, 'size': size}


@cache
def _find_source_bag(bag: str) -> str:
    """ find a bag in S3 returning path and bag name"""
    for location in SOURCE_BAG_LOCATIONS:
        key = f'{location}/{bag}/bagit.txt'
        try:
            s3_client = get_s3_client()
            s3_client.head_object(Bucket=S3_BUCKET, Key=key)
            return {"location": f'{location}/{bag}'}
        except ClientError as e:
            continue  # try next location
    raise NotFoundError('Could not find bag matching request!')


def _s3_byte_stream(bucket: str, key: str) -> BinaryIO:
    """ return an S3 object's data as a BytesIO stream """
    s3_client = get_s3_client()
    try:
        return io.BytesIO(s3_client.get_object(Bucket=bucket, Key=key)['Body'].read())
    except ClientError:
        raise NotFoundError('Could not access object!')


@cache
def _object_size(bucket: str, key: str) -> int:
    """ return the file size of an S3 object in bytes """
    s3_client = get_s3_client()
    try:
        return s3_client.head_object(Bucket=bucket, Key=key)['ContentLength']
    except ClientError:
        raise NotFoundError('Could not access object!')


@cache
def _is_file_too_large(file_sizes: int or tuple[int, ...], max_size: int = LAMBDA_MAX_MEMORY_FOR_DERIV, buffer_ratio: float = 0.3) -> bool:
    """
    check if enough memory is available based on memory size with a buffer ratio reserved for derivatives
    defaults to a memory size for derivative generation and a reservation of 30% of available memory
    """
    total_size = sum(file_sizes) if isinstance(file_sizes, tuple) else file_sizes
    return max_size * (1 - buffer_ratio) - total_size < 0


def _generate_pdf(bag: str, title: str = None, author: str = None, subject: str = None, keywords: str = None) -> dict:
    """ Generates PDF from default derivative images """
    destination = f'derivative/{bag}/pdf/{bag}.pdf'
    s3_client = get_s3_client()

    try:  # Test for existing pdf
        s3_client.head_object(Bucket=S3_BUCKET, Key=destination)
        logger.info(f'PDF already exists: {destination}')
        return {'message': 'PDF already exists'}
    except ClientError:
        pass  # does not exist - continue

    try:  # Test for existing derivatives
        image_paths, image_sizes = zip(
            *((item['file'], item['size'])  # returning tuples
              for item in images_derivative(bag, scale=DEFAULT_IMAGE_SCALE)
              )
        )
    except NotFoundError:
        logger.error('Missing derivative - failed to generate PDF')
        return {'message': 'missing derivative to generate PDF'}

    # Test total size of derivatives is under half of allocated memory size
    logger.debug(f'Total size of derivatives: {sum(image_sizes)}')
    logger.debug(f'LAMBDA_MAX_MEMORY_FOR_PDF: {LAMBDA_MAX_MEMORY_FOR_PDF}')
    if _is_file_too_large(image_sizes, max_size=LAMBDA_MAX_MEMORY_FOR_PDF, buffer_ratio=0.3):
        logger.error(f'Total size of derivatives is more than half of available memory: {sum(image_sizes)}')
        return {'message': 'Memory limit exceeded!'}

    # get contents of first image to bootstrap PDF generation
    image_path = image_paths[0]
    try:
        pdf = Image.open(_s3_byte_stream(bucket=S3_BUCKET, key=image_path))

        # save generated PDF back to S3
        pdf_file = io.BytesIO()
        pdf.save(
            pdf_file,
            format='PDF',
            save_all=True,
            title=title,
            author=author,
            subject=subject,
            keywords=keywords,
            append_images=(
                Image.open(_s3_byte_stream(bucket=S3_BUCKET, key=image_path))
                for image_path in image_paths[1:]  # skipping first image used to bootstrap pdf
            )
        )
    except UnidentifiedImageError:
        raise BadRequestError("An image is invalid for PDF generation.")
    pdf_file.flush()
    pdf_file.seek(0)
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=destination, Body=pdf_file)
    except:
        logger.error(f'Failed to save PDF to S3 for bag: {bag}')
        return {'message': 'failed to save PDF file'}
    logger.info(f'Generated PDF for bag: {bag}')
    return {'message': 'success'}


########################################################
# Event handlers

@app.on_sqs_message(queue=SQS_QUEUE_PDF, batch_size=1)
def pdf_generator(event) -> None:
    """ Watch for messages to generate PDF """
    for record in event:
        record_body = loads(record.body)
        logger.debug(record_body)
        try:
            _generate_pdf(**record_body)
        except (NotFoundError, BadRequestError) as e:
            logger.error(f"Failed to generate PDF: {record_body['bag']}")
        logger.debug(f"Created PDF: {record_body['bag']}")


@app.on_sqs_message(queue=SQS_QUEUE_DERIV, batch_size=1)
def deriv_generator(event) -> None:
    """ watch for messages to generate an image derivative """
    for record in event:
        record_body = record.body
        logger.debug(record_body)
        bag, scale, image, location = loads(record_body)
        try:
            resize_individual(bag, scale, image, location)
        except (NotFoundError, BadRequestError) as e:
            logger.error(f"Failed to resize: {bag}, {image}")
        logger.debug(f"Resized: {bag}, {image} -> {scale}, {location}")


########################################################
# Exposed functions

@app.route('/find/{bag}')
def find_source_bag(bag: str) -> str:
    return _find_source_bag(bag)


@app.route('/images/source/{bag}')
def images_source(bag: str, location_and_bag: str = "") -> list[dict]:
    """ API endpoint to list available source images and file sizes """
    location_and_bag = _find_source_bag(bag)['location'] if not location_and_bag else location_and_bag
    return list(_images(f'{location_and_bag}/data/'))


@app.route('/images/derivatives/{bag}/{scale}')
def images_derivative(bag: str, scale: float = DEFAULT_IMAGE_SCALE) -> list[dict]:
    """ API endpoint to list available images at specified scale """
    return list(_images(f'derivative/{bag}/{scale}/', extensions=DEFAULT_IMAGE_EXTENSIONS + ('pdf',)))


@app.route('/images/derivatives/{bag}')
def available_derivatives(bag: str) -> list[str]:
    """ API endpoint to list available derivative scales """
    s3_client = get_s3_client()
    # FIXME: this will get the first 1000 items matched against prefix and may miss some results
    try:
        return list(
            set(
                [
                    item['Key'].split('/')[2]
                    for item in s3_client.list_objects(Bucket=S3_BUCKET, Prefix=f'derivative/{bag}/')['Contents']
                ]
            )
        )
    except KeyError:
        raise NotFoundError('No derivatives found')


@app.route('/pdf/{bag}', methods=['GET', 'POST'])
def generate_pdf(bag: str) -> dict:
    """ API endpoint for requesting PDF generation """
    request = app.current_request
    data = request.json_body if request.json_body else {}
    pdf_queue = get_pdf_queue()
    logger.debug(f'Using queue: {pdf_queue}')
    logger.info(f'Processing {bag}')
    resp = pdf_queue.send_message(
        MessageBody=dumps(
            {
                'bag': bag,
                'title': data.get('title'),
                'author': data.get('author'),
                'subject': data.get('subject'),
                'keywords': data.get('keywords')
            }
        )
    )
    logger.debug(resp)
    return {'message': 'submitted for processing'}


@app.route('/resize/{bag}/{scale}/{image_path}')
def resize_individual(bag: str, scale: float, image_path: str, location: str = "") -> dict:
    """ API endpoint to resize specific image """
    deriv_image_path = Path(image_path).with_suffix('.jpg')
    destination = f'derivative/{bag}/{scale}/{deriv_image_path}'
    s3_client = get_s3_client()

    try:  # Test for existing derivative
        s3_client.head_object(Bucket=S3_BUCKET, Key=destination)
        logger.info(f'Derivative already exists: {destination}')
        return {'message': 'image already exists'}
    except ClientError:
        pass  # does not exist - continue

    bag_location = location if location else bag
    if _is_file_too_large(_object_size(S3_BUCKET, f'{bag_location}/data/{image_path}'),  max_size=LAMBDA_MAX_MEMORY_FOR_DERIV):
        raise BadRequestError("The source image is too large")

    try:
        source_image = Image.open(_s3_byte_stream(S3_BUCKET, f'{bag_location}/data/{image_path}'))
    except Exception as e:
        logger.error(f'Failed to open source image: {image_path}')
        logger.error(traceback.format_exc())
        return {'message': 'error opening source image'}

    size = (x * float(scale) for x in source_image.size)
    source_image.thumbnail(size, Image.ANTIALIAS)

    image_file = io.BytesIO()
    source_image.save(image_file, format='JPEG')
    image_file.flush()
    image_file.seek(0)

    s3_client.put_object(Bucket=S3_BUCKET, Key=destination, Body=image_file)
    logger.info(f'Created S3 object: {destination}')
    return {'message': 'created resized image'}


@app.route('/resize/{bag}/{scale}')
def resize(bag: str, scale: float) -> dict:
    """ API endpoint to resize images for specified bag """
    deriv_queue = get_deriv_queue()
    logger.debug(f'Using queue: {deriv_queue}')
    logger.info(f'Processing {bag}')
    location = _find_source_bag(bag)['location']
    logger.info(f'Using location {location}')
    for image_details in images_source(bag, location):
        size = image_details.get('size')
        image_filename = image_details.get("file").split('/')[-1]
        if _is_file_too_large(size):
            logger.error(f"{image_filename} from {bag} is too large to process!")
        else:
            resp = deriv_queue.send_message(
                MessageBody=dumps((bag, scale, image_filename, location))
            )
            logger.debug(resp)
    return {'message': 'submitted for processing'}


@app.route('/resize/{bag}/default')
def resize_default(bag: str) -> dict:
    """ API endpoint to resize images by default scale for specified bag """
    return resize(scale=DEFAULT_IMAGE_SCALE, bag=bag)
