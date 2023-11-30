import os
from pathlib import Path
import random
import shutil
import uuid

from aiobotocore.session import (
    get_session, AioSession)
from types_aiobotocore_s3.client import S3Client

import tests.test_s3 as test_s3

# Test settings
pwd: str = os.getcwd()
source: str = os.path.join(pwd, 'source')

async def download(client: S3Client,
                   bucket: str, folder: str, file: str):
    basename: str = os.path.basename(file)
    path: str = os.getcwd()
    os.chdir(folder)
    response = await client.get_object(
        Bucket=bucket,
        Key=file,
        ChecksumMode='ENABLED',
    )
    with open(basename, 'wb') as f:
        for chunk in await response.get('Body').iter_chunks(
                chunk_size=test_s3.CHUNK_SIZE):
            f.write(chunk)
    os.chdir(path)
    return response


def create_temp_file(size, file_name):
    random_file_name = file_name + "-" + str(uuid.uuid4().hex[:6])
    with open(random_file_name, 'wb') as f:
        f.write(os.urandom(size))
    return random_file_name


def create_temp_dir(parent):
    random_dir_name = str(uuid.uuid4().hex[:6])
    path = os.path.join(parent, random_dir_name)
    os.mkdir(path)
    return path


def create_dir_structure(root, dirs, subs, files):
    for i in range(dirs):
        dir = create_temp_dir(root)
        os.chdir(root)
        file = "file{}".format(i)
        create_temp_file(random.randrange(
            test_s3.MIN_FILE_SIZE, test_s3.MAX_FILE_SIZE, 1), file)
        for j in range(subs):
            sub = create_temp_dir(dir)
            path = os.path.join(root, dir)
            os.chdir(path)
            file = "file{}.{}".format(i, j)
            create_temp_file(random.randrange(
                test_s3.MIN_FILE_SIZE, test_s3.MAX_FILE_SIZE, 1), file)
            for k in range(files):
                path = os.path.join(root, dir, sub)
                os.chdir(path)
                file = "file{}.{}.{}".format(i, j, k)
                create_temp_file(random.randrange(
                    test_s3.MIN_FILE_SIZE, test_s3.MAX_FILE_SIZE, 1), file)
                
async def clean_up_s3(client: S3Client, bucket: str, folder: str) -> None:
    session: AioSession = get_session()
    async with session.create_client('s3') as client:
        objects_to_delete = await client.list_objects(
            Bucket=bucket, Prefix=folder)

        delete_keys = {'Objects': []}  # type: ignore
        delete_keys['Objects'] = [
            {'Key': k} for k in [
                obj['Key'] for obj in objects_to_delete.get(  # type: ignore
                    'Contents', [])]]  # type: ignore

        await client.delete_objects(
            Bucket=bucket, Delete=delete_keys)  # type: ignore


def clean_up_dir(dir):
    shutil.rmtree(dir)


def setup(fixtures: dict[str, bool | tuple]) -> tuple[str, str | None]:
    
    rand = str(uuid.uuid4().hex[:6])
    path: str = source + rand 
    Path(path).mkdir(exist_ok=True)
    if fixtures["status_file"] == True:
        status_file: str | None = "status"+rand+".json"
    else:
        status_file = None
    dirs: int = 0
    subs: int = 0
    files: int = 0
    if type(fixtures["dirs"]) == tuple:
        dirs, subs, files = fixtures["dirs"]
    else:
        raise ValueError("missing fixtures for dirs")
    
    create_dir_structure(path, dirs, subs, files)
    os.chdir(pwd)

    return path, status_file


async def teardown(teardown: dict[str, bool | str | S3Client | None]):
    os.chdir(pwd)
    clean_up_dir(teardown["source"])
    if teardown["client"] is not None:
        await clean_up_s3(teardown["client"], test_s3.bucket_name, "/") # type: ignore
    if teardown["status_file"] is not None:
        os.remove(str(teardown["status_file"]))