import asyncio
import json
import logging
import os
import hashlib
import base64
import sys
from typing import Any, Union

from aiobotocore.session import (
    get_session, AioSession)
from types_aiobotocore_s3.client import S3Client

from botocore import exceptions

BUF_SIZE = 65536
bucket_name = 'gb-upload'
Status_Details = dict[str, int|str] 
Status = dict[str, Status_Details]

async def worker(
        client: S3Client,
        bucket: str,
        queue: asyncio.Queue,
        files: dict[str, str],
        status_file: str,
        logger: str,
):
    file, state, func = await queue.get()
    if func == uploader:
        logging.info(f"{logger} | File {file} to be uploaded by worker")
        await uploader(client, bucket, file, state, files, status_file, logger)
    if func == hasher:
        logging.info(f"{logger} | File {file} to be hashed by worker")
        await hasher(file, state, files, status_file, logger)


async def uploader(
        client: S3Client,
        bucket: str,
        file: str,
        state: str,
        files: dict[str, str],
        status_file: str,
        logger: str,
):
    if state == "Done":
        logging.info(f"{logger} | File {file} already uploaded; skipping")
        return
    elif state == "":
        logging.info(f"{logger} | File {file} has no sha256; skipping")
    try:
        logging.info(
            f"{logger} | Trying to upload object {file}"
            " to S3 with sha {state}")
        response = await get_object_sha256(client, bucket, file, logger)
        if response.get("ChecksumSHA256") == state:
            raise FileExistsError
    except FileExistsError:
        logging.info(f"{logger} | File {file} already exists in S3; skipping")
    except exceptions.ClientError:
        with open(file, 'rb') as f:
            await client.put_object(
                Bucket=bucket,
                Body=f,
                Key=file,
                ChecksumAlgorithm='SHA256',
                ChecksumSHA256=state)
            logging.info(
                f"{logger} | File {file} successfully uploaded"
                " to S3 with sha256 {state}"
            )
            files[file] = "Done"
            save_status(files, status_file, logger)


async def upload(
        client: S3Client,
        bucket: str,
        file: str,
        sha256: str,
        logger: str) -> None:
    if sha256 == "Done":
        logging.info(f"{logger} | File {file} already uploaded; skipping")
        return
    try:
        logging.info(
            f"{logger} | Trying to upload object {file}"
            " to S3 with sha {sha256}")
        response = await get_object_sha256(client, bucket, file, logger)
        if response.get("ChecksumSHA256") == sha256:
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
                f"{logger} | File {file} successfully uploaded"
                " to S3 with sha256 {sha256}"
            )


async def get_object_sha256(
        client: S3Client, bucket: str, file: str, logger: str):
    try:
        logging.info(f"{logger} | Trying to get object {file} sha256 from S3")
        response = await client.head_object(
            Bucket=bucket,
            Key=file,
            ChecksumMode='ENABLED'
        )
    except exceptions.ClientError:
        logging.warning(f"{logger} | Object {file} not found in S3")
        raise
    sha256: str = response.get("ChecksumSHA256")
    logging.info(f"{logger} | Object {file} sha256: {sha256}")
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


def get_local_status(path: str, max_size: int) -> Status:
    logging.info(f"Getting local status with path:{path} & max_size: {max_size}")
    conf: Status_Details = { "max_file_size" : max_size }
    files: Status_Details = {}
    status_out: Status = {
        "conf" : conf,
        "files" : files,
    }
    for root, _, walk_files in os.walk(path):
        for name in walk_files:
            fullpath: str = os.path.join(root, name)
            statinfo: os.stat_result = os.stat(fullpath)
            if statinfo.st_size > max_size:
                logging.info(
                    f"File {fullpath} file_size {statinfo.st_size}"
                    f"> max_size {max_size}; skipping ")
                continue
            files[fullpath] = ""
            logging.info(f"Adding {fullpath} key with empty value")
    return status_out


async def set_hash(status: Status, status_file: str) -> None:
    files: Status_Details = status["files"]
    for file, state in files.items():
        if state != "":
            logging.info(f"File {file} has already been hashed, skipping")
            continue
        sha256: str = await hash(file)
        logging.info(f"Updating {file} key with value {sha256}")
        files[file] = sha256
        logger = "main"
        save_status(files, status_file, logger)


async def hasher(
        file: str,
        state: str,
        files: dict[str, str],
        status_file: str,
        logger: str):
    if state != "":
        logging.info(
            f"{logger} | File {file} has already been hashed, skipping")
    sha256: str = await hash(file)
    logging.info(f"Updating {file} key with value {sha256}")
    files[file] = sha256
    save_status(files, status_file, logger)


def get_local_files(path: str, logger: str) -> dict[str, str]:
    logging.info(f"{logger} | Getting local files")
    file_out: dict[str, str] = {}
    for root, _, files in os.walk(path):
        for name in files:
            fullpath: str = os.path.join(root, name)
            file_out[fullpath] = ""
            logging.debug(f"{logger} | Adding {fullpath} key with empty value")
    return file_out


async def add_files_to_queues(
        files: dict[str, str],
        queue: asyncio.Queue,
        logger: str):
    # TODO: shift to only one queue
    for file, state in files.items():
        if state == "":
            logging.debug(f"{logger} | Adding file {file} to hash queue")
            await queue.put((file, state, hasher))
        elif state == "Done":
            logging.debug(f"{logger} | File {file} is already done")
            continue
        else:
            logging.debug(f"{logger} | Adding file {file} to upload queue")
            await queue.put((file, state, uploader))


def save_status(files: Status_Details, status_file: str) -> None:
    with open(status_file, 'w') as f:
        f.write(json.dumps(files, indent=2))
        logging.info(f"{logger} | Writing status to {status_file}")


def load_status(status_file: str, logger: str) -> dict[str, str]:
    try:
        with open(status_file, 'r') as json_file:
            logging.info(f"{logger} | Loading status from {status_file}")
            return json.load(json_file)
    except FileNotFoundError:
        raise


def check_status(source, status_file, logger) -> dict[str, str]:
    files: dict[str, str] = {}
    try:
        files = load_status(status_file, logger)
    except FileNotFoundError:
        files = get_local_status(source, max_size)
        save_status(files, status_file)
    return files


async def main(
        source: str,
        status_file: str,
        num_workers: int,
) -> None:
    logging.basicConfig(
        format='%(asctime)s | %(levelname)s | %(message)s',
        filename='s3upload.log', level=logging.INFO)
    logger = "main"
    logging.info(f'{logger} | Started s3uploader')
    logging.info(f"{logger} | Source set to {source}")
    logging.info(f"{logger} | Status file set to {status_file}")

    files: dict[str, str] = check_status(source, status_file, max_size)


    # await set_hash(files, status_file)

    session: AioSession = get_session()
    async with session.create_client('s3') as client:
        worker_logger: str = "worker-"
        workers = [asyncio.create_task(
            worker(
                client,
                bucket_name,
                queue,
                files,
                status_file,
                worker_logger + str(n)) for n in range(num_workers)
        )
        ]

        await asyncio.gather(*workers)

    logging.info(f'{logger} | Finished')


"""         for file, sha256 in files.items():
            try:
                await upload(client, bucket_name, file, sha256)
            except FileExistsError:
                logging.info(f"File {file} already exists in S3; skipping")
            files[file] = "Done"
            save_status(files, status_file) """


if __name__ == '__main__':
    if len(sys.argv) < 5:
        logging.basicConfig(
            format='%(asctime)s | %(levelname)s | %(message)s',
            filename='s3upload.log', level=logging.INFO)
        logging.error("Missing arguments")
        print("Usage: source_path status_file log_file num_workers")
        sys.exit()
    else:
        logging.basicConfig(
            format='%(asctime)s | %(levelname)s | %(message)s',
            filename=sys.argv[3], level=logging.INFO)
    if type(sys.argv[4]) is not int:
        logging.error("Arg 3 must be an int")
        print("Usage: source_path status_file num_workers")
        sys.exit()

    asyncio.run(main(sys.argv[1], sys.argv[2],
                int(sys.argv[4])))
