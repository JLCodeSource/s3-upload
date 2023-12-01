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

def skip_file(file, status) -> bool:
    logging.info(f"Checking File {file} status")
    # skip files that are Uploaded or Suspect
    if status == "Uploaded" or status == "Suspect":
        logging.info(f"File {file} is {status}; skipping")
        return True
    else:
        logging.info(f"File {file} status is {status}; continuing")
        return False
    

async def upload(
        client: S3Client,
        bucket: str,
        file: str,
        status: str) -> str:
    if skip_file(file, status) is True:
        return status
    try:
        logging.info(
            f"Trying to upload object {file} to S3 with sha256 {status}")
        # get_object_sha256 raises a ClientError if no file exists
        response = await get_object_sha256(client, bucket, file)
        if response.get("ChecksumSHA256") == status:
            raise FileExistsError
        # If the FileExists we raise above & if not, 
        # we get a ClientError from get_object_sha256 & handle below 
        return "Should Not Occur"
    # if get_object_sha256 raises a ClientError upload file
    except exceptions.ClientError:
        #try:
        logging.info(f"Attempting to upload {file} with sha256 {status}")
        with open(file, 'rb') as f:
            await client.put_object(
                Bucket=bucket,
                Body=f,
                Key=file,
                ChecksumAlgorithm='SHA256',
                ChecksumSHA256=status)
            logging.info(
                f"File {file} successfully uploaded to S3 with sha256 {status}"
            )
        return "Uploaded"
    #except FileNotFoundError as err:
        #    raise err


async def get_object_sha256(
        client: S3Client, bucket: str, file: str):
    try:
        logging.info(f"Trying to get object {file} sha256 from S3")
        response = await client.head_object(
            Bucket=bucket,
            Key=file,
            ChecksumMode='ENABLED'
        )
    # raise ClientError if object not found
    except exceptions.ClientError as err:
        logging.info(f"Object {file} not found in S3; {err}")
        raise
    sha256: str = response.get("ChecksumSHA256")
    logging.info(f"Object {file} sha256: {sha256}")
    return response


async def hash(file: str) -> str:
    sha256: hashlib._Hash = hashlib.sha256()
    logging.info(f"Attempting to hash file {file}")
    try:
        with open(file, 'rb') as f:
            while True:
                data: bytes = f.read(BUF_SIZE)
                if not data:
                    break
                sha256.update(data)
    # raise OSError for caller to handle if IO error
    except OSError as err:
        logging.error(f"File {file} raised OSError: {err}")
        raise
    digest: str = base64.b64encode(sha256.digest()).decode()
    logging.info(f"File {file} has sha256 {digest}")
    return digest


def get_local_files(path: str, max_size: int) -> dict[str, str]:
    logging.info(f"Getting local files with max_size: {max_size}")
    file_out: dict[str, str] = {}
    for root, _, files in os.walk(path):
        for name in files:
            fullpath: str = os.path.join(root, name)
            statinfo: os.stat_result = os.stat(fullpath)
            # check file size
            if statinfo.st_size > max_size:
                logging.info(
                    f"File {fullpath} file_size {statinfo.st_size}"
                    f"> max_size {max_size}; skipping ")
                continue
            file_out[fullpath] = ""
            logging.info(f"Adding {fullpath} key with empty value")
    return file_out


async def set_hash(files: dict[str, str], status_file: str) -> None:
    for file, state in files.items():
        if state != "":
            logging.info(f"File {file} has already been hashed; skipping")
            continue
        try:
            sha256: str = await hash(file)
        # tag files with IO Errors with Suspect
        except OSError:
            logging.info(f"File {file} raised OSError; tagging with 'Suspect' & skipping")
            files[file] = "Suspect"
            save_status(files, status_file)
            continue
        logging.info(f"Updating {file} key with value {sha256}")
        files[file] = sha256
        save_status(files, status_file)


async def add_files_to_queues(
        files: dict[str, str],
        hash_q: asyncio.Queue,
        upload_q: asyncio.Queue):
    for file, state in files.items():
        if state == "":
            logging.info(f"Adding file {file} to hash queue")
            await hash_q.put(file)
        elif state == "Uploaded":
            logging.info(f"File {file} is already Uploaded")
            continue
        elif state == "Suspect":
            logging.info(f"File {file} is suspect; skipping")
            continue
        else:
            logging.info(f"Adding file {file} to upload queue")
            await upload_q.put(file)


def save_status(files: dict[str, str], status_file: str) -> None:
    with open(status_file, 'w') as f:
        f.write(json.dumps(files, indent=2))
        logging.info(f"Writing status to {status_file}")


def load_status(status_file: str) -> dict[str, str]:
    try:
        with open(status_file, 'r') as json_file:
            logging.info(f"Loading status from {status_file}")
            return json.load(json_file)
    except FileNotFoundError:
        raise


def check_status(source, status_file, max_size) -> dict[str, str]:
    files: dict[str, str] = {}
    try:
        files = load_status(status_file)
    except FileNotFoundError:
        files = get_local_files(source, max_size)
        save_status(files, status_file)
    return files


async def main(source: str, status_file: str, max_size: int) -> None:
    logging.info('Started s3uploader')
    logging.info(f"Source set to {source}")
    logging.info(f"Status file set to {status_file}")

    files: dict[str, str] = check_status(source, status_file, max_size)

    await set_hash(files, status_file)

    session: AioSession = get_session()
    async with session.create_client('s3') as client:
        for file, status in files.items():
            new_status = ""
            try:
                new_status: str = await upload(client, bucket_name, file, status)
            except FileExistsError:
                logging.info(f"File {file} already exists in S3; skipping")
            #except FileNotFoundError as err:
            #    logging.error(f"File {file} not found with error: {err}; skipping")
            files[file] = new_status
            save_status(files, status_file)

    logging.info('Finished s3uploader')


if __name__ == '__main__':
    default_log_file = 's3upload.log'
    usage = "Usage: python s3.py source_dir status_file max_size [log_file]"
    if len(sys.argv) < 4:
        print(f"ERROR: Missing arguments - {usage}")
        sys.exit()
    elif len(sys.argv) == 4:
        logging.basicConfig(
            format='%(asctime)s | %(levelname)s | %(message)s',
            filename=default_log_file, level=logging.INFO)
    elif len(sys.argv) >= 5:
        log_file: str = sys.argv[4]
        logging.basicConfig(
            format='%(asctime)s | %(levelname)s | %(message)s',
            filename=log_file, level=logging.INFO)

    if len(sys.argv) > 5:
        logging.error(f"Too many args - {usage}")
        print(f"Too many args - {usage}")
        sys.exit()

    try:
        max_size = int(sys.argv[3])
    except ValueError:
        logging.error(f"Wrong type for max_size - {usage}")
        print(f"Wrong type for max_size - {usage}")
        sys.exit()

    source: str = sys.argv[1]

    if not os.path.exists(source):
        logging.error(f"Source does not exist - {usage}")
        print(f"Source does not exist - {usage}")
        sys.exit()

    asyncio.run(main(sys.argv[1], sys.argv[2], int(sys.argv[3])))
