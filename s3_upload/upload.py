import boto3
import os
import hashlib
import base64
import sys

from mypy_boto3_s3 import S3ServiceResource

BUF_SIZE = 65536
s3: S3ServiceResource = boto3.resource('s3')
bucket_name = 'gb-upload'
bucket = s3.Bucket(name=bucket_name)


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


def getObjSha(resource: S3ServiceResource, bucket: str, file: str):
    response = s3.meta.client.head_object(
        Bucket=bucket,
        Key=file,
        ChecksumMode='ENABLED'
    )
    return response


def download(resource: S3ServiceResource, bucket: str, folder: str, file: str):
    basename = os.path.basename(file)
    path = os.getcwd()
    os.chdir(folder)
    response = resource.meta.client.get_object(
        Bucket=bucket,
        Key=file,
        ChecksumMode='ENABLED',
    )
    with open(basename, 'wb') as f:
        for chunk in response.get('Body').iter_chunks(chunk_size=65536):
            f.write(chunk)
    os.chdir(path)
    return response


def delete(resource, bucket, file):
    resource.Object(bucket, file).delete()
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


def process_file(file, folder):
    sha256 = hash(file)
    print(sha256)
    print(file)

    upload(s3, bucket_name, file, sha256)
    download(s3, bucket_name, folder, file)
    # delete(s3, bucket_name, file)


def walk(path):
    file_list = []
    for root, dirs, files in os.walk(path):
        for name in files:
            file_list.append(os.path.join(root, name))
    return file_list


def main(source, target):
    if len(sys.argv) <= 2:
        print("Provide source path")
        sys.exit()
    elif source == "":
        source = sys.argv[1]
    elif target == "":
        target = sys.argv[2]

    files = walk(source)

    for file in files:
        process_file(file, target)


main("", "")
