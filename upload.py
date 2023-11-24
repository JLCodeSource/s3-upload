import boto3
import uuid
import random
import string
import os
import hashlib
import base64

BUF_SIZE = 65536000
s3 = boto3.resource('s3')
bucket_name = 'gb-upload'
bucket = s3.Bucket(name=bucket_name)


def create_temp_file(size, file_name):
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    letters = string.ascii_lowercase
    random_content = ''.join(random.choice(letters) for i in range(size))
    with open(random_file_name, 'w') as f:
        f.write(str(random_content))
    return random_file_name


def upload(resource, bucket, file, sha256):
    with open(file, 'rb') as f:
        if sha256 == '':
            resource.meta.client.put_object(
                Bucket=bucket,
                Body=f,
                Key=file,
                ChecksumAlgorithm='SHA256')
        else:
            resource.meta.client.put_object(
                Bucket=bucket,
                Body=f,
                Key=file,
                ChecksumAlgorithm='SHA256',
                ChecksumSHA256=sha256)


def download(resource, bucket, file):
    resource.Object(bucket, file).download_file(
        f'./tmp/{file}')


def delete(resource, bucket, file):
    # resource.Object(bucket, file).delete()
    os.remove(file)


def hash(file):
    sha256 = hashlib.sha256()
    with open(file, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)
    return base64.b64encode(sha256.digest()).decode()


file = create_temp_file(5, 'file.txt')

sha256 = hash(file)
# sha256_hex = sha256.hexdigest()
print(sha256)
print(file)

upload(s3, bucket_name, file, sha256)
download(s3, bucket_name, file)
# delete(s3, bucket_name, file)


for b in s3.buckets.all():
    print(b)
