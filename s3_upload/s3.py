import boto3
import os
import hashlib
import base64
import sys

from mypy_boto3_s3 import S3ServiceResource

BUF_SIZE = 65536
resource: S3ServiceResource = boto3.resource('s3')
bucket_name = 'gb-upload'


def upload(
        resource: S3ServiceResource,
        bucket: str,
        file: str,
        sha256: str) -> None:
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


def getObjectSha(resource: S3ServiceResource, bucket: str, file: str):
    response = resource.meta.client.head_object(
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


def hash(file: str) -> str:
    sha256 = hashlib.sha256()
    with open(file, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)
    return base64.b64encode(sha256.digest()).decode()


def get_local_files(path: str) -> dict[str, str]:
    file_out: dict[str, str] = {}
    for root, dirs, files in os.walk(path):
        for name in files:
            file_out[os.path.join(root, name)] = ""
    return file_out


def set_hash(files) -> None:
    for file in files:
        sha256: str = hash(file)
        files[file] = sha256


def main(source: str, target: str) -> None:
    if len(sys.argv) <= 2:
        print("Provide source path")
        sys.exit()
    elif source == "":
        source = sys.argv[1]
    elif target == "":
        target = sys.argv[2]

    files: dict[str, str] = get_local_files(source)

    set_hash(files)

    for file, sha in files.items():
        upload(resource, bucket_name, file, sha)
        download(resource, bucket_name, target, file)


main("", "")
