import asyncio
import json
import os
import hashlib
import base64
import sys

from aiobotocore.session import (  # type: ignore
    get_session, AioSession, AioBaseClient)
from botocore import exceptions

BUF_SIZE = 65536
bucket_name = 'gb-upload'


async def upload(
        client: AioBaseClient,
        bucket: str,
        file: str,
        sha256: str) -> None:
    try:
        response = await get_object_sha256(client, bucket, file)
        if response.get("ChecksumSHA256") == sha256:
            raise FileExistsError
    except exceptions.ClientError:
        with open(file, 'rb') as f:
            await client.put_object(
                Bucket=bucket,
                Body=f,
                Key=file,
                ChecksumAlgorithm='SHA256',
                ChecksumSHA256=sha256)  # type: ignore


async def get_object_sha256(
        client: AioBaseClient, bucket: str, file: str):
    try:
        response = await client.head_object(
            Bucket=bucket,
            Key=file,
            ChecksumMode='ENABLED'
        )  # type: ignore
    except exceptions.ClientError:
        raise
    return response


async def hash(file: str) -> str:
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


async def set_hash(files) -> None:
    for file in files:
        sha256: str = await hash(file)
        files[file] = sha256


def save_status(files: dict[str, str], status_file: str) -> None:
    with open(status_file, 'w') as f:
        f.write(json.dumps(files, indent=2))


def check_status(status_file: str) -> dict[str, str]:
    with open(status_file, 'r') as json_file:
        return json.load(json_file)


async def main(source: str, target: str) -> None:
    if len(sys.argv) <= 2:
        print("Provide source path")
        sys.exit()
    elif source == "":
        source = sys.argv[1]
    elif target == "":
        target = sys.argv[2]

    files: dict[str, str] = get_local_files(source)

    await set_hash(files)

    session: AioSession = get_session()
    async with session.create_client('s3') as client:
        for file, sha256 in files.items():
            await upload(client, bucket_name, file, sha256)

    save_status(files, "status.json")


asyncio.run(main("", ""))
