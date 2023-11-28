import asyncio
import json
import logging
import os
import hashlib
import base64
import sys

from aiobotocore.session import (
    get_session, AioSession)
from types_aiobotocore_s3.client import S3Client

from botocore import exceptions

BUF_SIZE = 65536
bucket_name = 'gb-upload'


async def upload(
        client: S3Client,
        bucket: str,
        file: str,
        sha256: str) -> None:
    try:
        logging.info(
            f"Trying to upload object {file} to S3 with sha {sha256}")
        response = await get_object_sha256(client, bucket, file)
        if response.get("ChecksumSHA256") == sha256:
            logging.warning(
                f"Object {file} exists in S3"
            )
            raise FileExistsError
    except exceptions.ClientError:
        with open(file, 'rb') as f:
            await client.put_object(
                Bucket=bucket,
                Body=f,
                Key=file,
                ChecksumAlgorithm='SHA256',
                ChecksumSHA256=sha256)
            logging.info(
                f"File {file} successfully uploaded to S3 with sha {sha256}"
            )


async def get_object_sha256(
        client: S3Client, bucket: str, file: str):
    try:
        logging.info(f"Trying to get object {file} sha256 from S3")
        response = await client.head_object(
            Bucket=bucket,
            Key=file,
            ChecksumMode='ENABLED'
        )
    except exceptions.ClientError:
        logging.warning(f"Object {file} not found in S3")
        raise
    sha256: str = response.get("ChecksumSHA256")
    logging.info(f"Object {file} sha256: {sha256}")
    return response


async def hash(file: str) -> str:
    sha256: hashlib._Hash = hashlib.sha256()
    logging.info(f"Attempting to hash file {file}")
    with open(file, 'rb') as f:
        while True:
            data: bytes = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)
    digest: str = base64.b64encode(sha256.digest()).decode()
    logging.info(f"File {file} has sha256 {digest}")
    return digest


def get_local_files(path: str) -> dict[str, str]:
    logging.info("Getting local files")
    file_out: dict[str, str] = {}
    for root, _, files in os.walk(path):
        for name in files:
            fullpath: str = os.path.join(root, name)
            file_out[fullpath] = ""
            logging.debug(f"Adding {fullpath} key with empty value")
    return file_out


async def set_hash(files: dict[str, str]) -> None:
    for file in files:
        sha256: str = await hash(file)
        logging.debug(f"Updating {file} key with value {sha256}")
        files[file] = sha256


async def add_files_to_queues(
        files: dict[str, str],
        hash_q: asyncio.Queue,
        upload_q: asyncio.Queue):
    for file, state in files.items():
        if state == "":
            logging.debug(f"Adding file {file} to hash queue")
            await hash_q.put(file)
        elif state == "Done":
            logging.debug(f"File {file} is already done")
            continue
        else:
            logging.debug(f"Adding file {file} to upload queue")
            await upload_q.put(file)


def save_status(files: dict[str, str], status_file: str) -> None:
    with open(status_file, 'w') as f:
        f.write(json.dumps(files, indent=2))
        logging.info(f"Writing status to {status_file}")


def check_status(status_file: str) -> dict[str, str]:
    with open(status_file, 'r') as json_file:
        logging.info(f"Loading status from {status_file}")
        return json.load(json_file)


async def main(source: str, target: str) -> None:
    logging.basicConfig(
        format='%(asctime)s | %(levelname)s | %(message)s',
        filename='s3upload.log', level=logging.INFO)
    logging.info('Started s3uploader')
    if len(sys.argv) <= 2:
        logging.error("Missing arguments")
        print("Provide source path")
        sys.exit()
    elif source == "":
        source = sys.argv[1]
        logging.info(f"Source set to {source}")
    elif target == "":
        target = sys.argv[2]
        logging.info(f"Target set to {target}")

    files: dict[str, str] = get_local_files(source)

    await set_hash(files)

    session: AioSession = get_session()
    async with session.create_client('s3') as client:
        for file, sha256 in files.items():
            await upload(client, bucket_name, file, sha256)

    save_status(files, "status.json")

    logging.info('Finished')

asyncio.run(main("", ""))
