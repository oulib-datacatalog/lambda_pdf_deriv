import io
import logging

from json import dumps, loads
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from chalice import Chalice, NotFoundError
from PIL import Image

DEFAULT_IMAGE_SCALE = 0.4  # of original size
DEFAULT_IMAGE_EXTENSIONS = ('jpg', 'tif', 'tiff', 'png')

LAMBDA_MAX_MEMORY_FOR_PDF = 2147483648  # 2GB

SQS_QUEUE_DERIV = 'infx_deriv_gen'
SQS_QUEUE_PDF = 'infx_pdf_gen'
S3_BUCKET = 'tdp-bagit'

app = Chalice(app_name='lambda_pdf_deriv')
app.debug = False
app.log.setLevel(logging.WARN)

s3_client = boto3.client('s3')
s3_paginator = s3_client.get_paginator('list_objects_v2')
sqs = boto3.resource('sqs')

deriv_queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_DERIV)
pdf_queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_PDF)


def _filter_keep(file, extensions=DEFAULT_IMAGE_EXTENSIONS, ignore_orig=True):
    ''' filters to apply to a file's name to determine to keep '''
    if not file.endswith(extensions):
        return False
    if (ignore_orig and 'orig' in Path(file).name.lower()):
        return False
    if Path(file).name.startswith('.'):
        return False
    return True


def _images(prefix, extensions=DEFAULT_IMAGE_EXTENSIONS, ignore_orig=True):
    ''' yield images in S3 with filtering '''
    for page in s3_paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        try:
            contents = page['Contents']
        except KeyError as e:
            app.log.error(f'Failed to access: {e}')
            raise NotFoundError('Could not find bag matching request!')
        for obj in contents:
            try:
                file = obj['Key']
                size = obj['Size']
            except KeyError as e:
                app.log.error(f'Failed to access: {e}')
                raise NotFoundError('Could not find images matching request!')
            
            if _filter_keep(file, extensions=extensions, ignore_orig=ignore_orig):
                yield {'file': file, 'size': size}


@app.route('/images/source/{bag}')
def images_source(bag):
    ''' API endpoint to list available source images '''
    return list(_images(f'source/{bag}/data/'))


@app.route('/images/derivatives/{bag}/{scale}')
def images_derivative(bag, scale=DEFAULT_IMAGE_SCALE):
    ''' API endpoint to list available images at specified scale '''
    return list(_images(f'derivative/{bag}/{scale}/', extensions=DEFAULT_IMAGE_EXTENSIONS + ('pdf',)))


@app.route('/images/derivatives/{bag}')
def available_derivatives(bag):
    ''' API endpoint to list available derivative scales '''
    # FIXME: this will get the first 1000 items matched against prefix and may miss some results
    return list(
        set(
            [
                item['Key'].split('/')[2]
                for item in s3_client.list_objects(Bucket=S3_BUCKET, Prefix=f'derivative/{bag}/')['Contents']
            ]
        )
    )


def _s3_byte_stream(bucket, key):
    ''' return an S3 object's data as a BytesIO stream '''
    return io.BytesIO(s3_client.get_object(Bucket=bucket, Key=key)['Body'].read())


def _generate_pdf(bag, title=None, author=None, subject=None, keywords=None):
    ''' Generates PDF from default derivative images '''
    destination = f'derivative/{bag}/pdf/{bag}.pdf'

    try:  # Test for existing pdf
        s3_client.head_object(Bucket=S3_BUCKET, Key=destination)
        app.log.info(f'PDF already exists: {destination}')
        return {'message': 'PDF already exists'}
    except ClientError:
        pass  # does not exist - continue
    
    try:  # Test for existing derivatives
        image_paths, image_sizes = zip(
            *((item['file'], item['size'])
              for item in images_derivative(bag, scale=DEFAULT_IMAGE_SCALE)
            )
        )
    except NotFoundError:
        app.log.error('Missing derivative - failed to generate PDF')
        return {'message': 'missing derivative to generate PDF'}

    # Test total size of derivatives is under half of allocated memory size
    total_deriv_size = sum(image_sizes)
    app.log.debug(f'Total size of derivatives: {total_deriv_size}')
    app.log.debug(f'LAMBDA_MAX_MEMORY_FOR_PDF: {LAMBDA_MAX_MEMORY_FOR_PDF}')
    if total_deriv_size > LAMBDA_MAX_MEMORY_FOR_PDF / 2:  # TODO: test memory usage to confirm setting
        app.log.error(f'Total size of derivatives is more than half of available memory: {total_deriv_size}')
        return {'message': 'memory limit exceeded'}
    
    # get contents of first image to bootstrap PDF generation
    image_path = image_paths[0]
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
    pdf_file.flush()
    pdf_file.seek(0)
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=destination, Body=pdf_file)
    except:
        app.log.error(f'Failed to save PDF to S3 for bag: {bag}')
    app.log.info(f'Generated PDF for bag: {bag}')


@app.on_sqs_message(queue=SQS_QUEUE_PDF, batch_size=1)
def pdf_generator(event):
    ''' Watch for messages to generate PDF '''
    for record in event:
        record_body = loads(record.body)
        app.log.debug(record_body)
        _generate_pdf(**record_body)


@app.route('/pdf/{bag}', methods=['GET', 'POST'])
def generate_pdf(bag):
    ''' API endpoint for requesting PDF generation '''
    request = app.current_request
    data = request.json_body if request.json_body else {}
    app.log.debug(f'Using queue: {pdf_queue}')
    app.log.info(f'Processing {bag}')
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
    app.log.debug(resp)
    return {'message': 'submitted for processing'}
        

@app.route('/resize/{bag}/{scale}/{image_path}')
def resize_individual(bag, scale, image_path):
    ''' API endpoint to resize specific image '''
    deriv_image_path = Path(image_path).with_suffix('.jpg')
    destination = f'derivative/{bag}/{scale}/{deriv_image_path}'
    
    try:  # Test for existing derivative
        s3_client.head_object(Bucket=S3_BUCKET, Key=destination)
        app.log.info(f'Derivative already exists: {destination}')
        return {'message': 'image already exists'}
    except ClientError:
        pass  # does not exist - continue
    
    try:
        source_image = Image.open(_s3_byte_stream(S3_BUCKET, f'source/{bag}/data/{image_path}'))
    except Exception as e:
        app.log.error(f'Failed to open source image: {image_path}')
        app.log.error(e)
        return {'message': 'error opening source image'}

    size = (x * float(scale) for x in source_image.size)
    source_image.thumbnail(size, Image.ANTIALIAS)

    image_file = io.BytesIO()
    source_image.save(image_file, format='JPEG')
    image_file.flush()
    image_file.seek(0)
    
    s3_client.put_object(Bucket=S3_BUCKET, Key=destination, Body=image_file)
    app.log.info(f'Created S3 object: {destination}')
    return {'message': 'created resized image'}


@app.on_sqs_message(queue=SQS_QUEUE_DERIV, batch_size=1)
def deriv_generator(event):
    ''' watch for messages to generate an image derivative '''
    for record in event:
        record_body = record.body
        app.log.debug(record_body)
        bag, scale, image = loads(record_body)
        resize_individual(bag, scale, image)


@app.route('/resize/{bag}/{scale}')
def resize(bag, scale):
    ''' API endpoint to resize images for specified bag 
        
    '''
    app.log.debug(f'Using queue: {deriv_queue}')
    app.log.info(f'Processing {bag}')
    for full_s3_key in images_source(bag):
        image = full_s3_key.split('/')[-1]
        resp = deriv_queue.send_message(
            MessageBody=dumps((bag, scale, image))
        )
        app.log.debug(resp)
    return {'message': 'submitted for processing'}


@app.route('/resize/{bag}/default')
def resize_default(bag):
    ''' API endpoint to resize images by default scale for specified bag '''
    return resize(scale=DEFAULT_IMAGE_SCALE, bag=bag)
