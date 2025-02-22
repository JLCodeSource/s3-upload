import asyncio
from dataclasses import dataclass, field
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

from dataclasses_json import dataclass_json

BUF_SIZE = 65536
bucket_name = 'gb-upload'


@dataclass_json
@dataclass
class File:
    filepath: str = field(default="")
    is_hashed: bool = field(default=False)
    is_uploaded: bool = field(default=False)
    is_suspect: bool = field(default=False)
    sha256: str = field(default="")


def skip_file(file: File) -> bool:
    logging.info(f"Checking File {file.filepath} status")
    # skip files that are Uploaded or Suspect
    if file.is_suspect:
        logging.info(f"File {file.filepath} status is Suspect; skipping")
        return True
    elif file.is_uploaded:
        logging.info(f"File {file.filepath} status is Uploaded; skipping")
        return True
    else:
        logging.info(f"File {file} status is ok; continuing")
        return False
    

async def upload(
        client: S3Client,
        bucket: str,
        file: File) -> File:
    if skip_file(file):
        return file
    try:
        logging.info(
            f"Trying to upload Object {file.filepath} to S3 with sha256 {file.sha256}")
        # get_object_sha256 raises a ClientError if no file exists
        response = await get_object_sha256(client, bucket, file.filepath)
        s3_hash: str = response.get("ChecksumSHA256")
        if s3_hash == file.sha256:
            raise FileExistsError (f"Object {file.filepath} exists in S3 with matching sha256 {file.sha256}")
        # If the FileExists we raise above & if not, 
        # we get a ClientError from get_object_sha256 & handle below
        logging.warning(f"Uploaded hash {s3_hash} does not match local hash {file.sha256}") 
        return file
    # if get_object_sha256 raises a ClientError upload file
    except exceptions.ClientError:
        try:
            logging.info(f"Attempting to upload {file.filepath} with sha256 {file.sha256}")
            with open(file.filepath, 'rb') as f:
                await client.put_object(
                    Bucket=bucket,
                    Body=f,
                    Key=file.filepath,
                    ChecksumAlgorithm='SHA256',
                    ChecksumSHA256=file.sha256)
                logging.info(
                    f"File {file.filepath} successfully uploaded to S3 with sha256 {file.sha256}"
                )
            file.is_uploaded = True
            return file
        except FileNotFoundError as err:
            raise err


async def get_object_sha256(
        client: S3Client, bucket: str, file: str):
    try:
        logging.info(f"Trying to get Object {file} sha256 from S3")
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
    logging.info(f"S3 Object {file} sha256: {sha256}")
    return response


async def hash(filepath: str) -> str:
    sha256: hashlib._Hash = hashlib.sha256()
    logging.info(f"Attempting to hash file {filepath}")
    try:
        with open(filepath, 'rb') as f:
            while True:
                data: bytes = f.read(BUF_SIZE)
                if not data:
                    break
                sha256.update(data)
    # raise OSError for caller to handle if IO error
    except OSError as err:
        logging.error(f"File {filepath} raised OSError: {err}")
        raise
    digest: str = base64.b64encode(sha256.digest()).decode()
    logging.info(f"File {filepath} has sha256 {digest}")
    return digest


def get_local_files(path: str, max_size: int) -> list[File]:
    logging.info(f"Getting local files with max_size: {max_size}")
    file_list: list[File] = []
    for root, _, files in os.walk(path):
        for name in files:
            fullpath: str = os.path.join(root, name)
            statinfo: os.stat_result = os.stat(fullpath)
            # check file size
            if statinfo.st_size > max_size:
                logging.info(
                    f"File {fullpath} file_size {statinfo.st_size}"
                    f"> max_size {max_size}; skipping")
                continue
            file_list.append(File(filepath=fullpath))
            logging.info(f"Adding {fullpath} key with default values")
    return file_list


async def set_hash(files: list[File], status_file: str) -> None:
    for file in files:
        if file.is_hashed:
            logging.info(f"File {file.filepath} has already been hashed; skipping")
            continue
        try:
            sha256: str = await hash(file.filepath)
        # tag files with IO Errors with Suspect
        except OSError:
            logging.info(f"File {file.filepath} raised OSError; tagging as Suspect & skipping")
            file.is_suspect = True
            save_status(files, status_file)
            continue
        logging.info(f"Updating {file.filepath} key with value {sha256} & setting is_hashed to True")
        file.sha256 = sha256
        file.is_hashed = True
        save_status(files, status_file)


async def add_files_to_queues(
        files: list[File],
        hash_q: asyncio.Queue,
        upload_q: asyncio.Queue):
    for file in files:
        if file.is_hashed is False:
            logging.info(f"Adding file {file.filepath} to hash queue")
            await hash_q.put(file)
        elif file.is_uploaded:
            logging.info(f"File {file.filepath} is already Uploaded")
            continue
        elif file.is_suspect:
            logging.info(f"File {file.filepath} is suspect; skipping")
            continue
        else:
            logging.info(f"Adding file {file.filepath} to upload queue")
            await upload_q.put(file)


def save_status(files: list[File], status_file: str) -> None:
    with open(status_file, 'w') as f:
        f.write(File.schema().dumps(files, many=True, indent=2)) # type: ignore
        logging.info(f"Writing status to {status_file}")


def load_status(status_file: str) -> list[File]:
    try:
        with open(status_file, 'r') as json_file:
            logging.info(f"Loading status from {status_file}")
            loaded = json.load(json_file)
            return File.schema().load(loaded, many=True) # type: ignore
    except FileNotFoundError:
        raise


def check_status(source, status_file, max_size) -> list[File]:
    files: list[File] = []
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

    files: list[File] = check_status(source, status_file, max_size)

    await set_hash(files, status_file)

    session: AioSession = get_session()
    async with session.create_client('s3') as client:
        for file in files:
            try:
                await upload(client, bucket_name, file)
                save_status(files, status_file)
            except FileNotFoundError or OSError as err:
                file.is_suspect = True
                save_status(files, status_file)
                logging.warning(f"File {file.filepath} upload errored with {err}; skipping")
                continue
            except FileExistsError as err:
                file.is_uploaded = True
                save_status(files, status_file)
                logging.warning(f"File {file.filepath} upload errored with {err}; skipping")
                continue
            
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
