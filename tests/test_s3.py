import shutil
import uuid
import os
import random
import boto3
import json
from pathlib import Path

from mypy_boto3_s3 import S3ServiceResource
from botocore import exceptions
import pytest
from s3_upload import s3


resource = boto3.resource('s3')
bucket_name = 'gb-upload'

# Test settings
pwd: str = os.getcwd()
source: str = os.path.join(pwd, 'source')
tmp: str = os.path.join(pwd, 'tmp')


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
            1024, (10*1024*1024), 1), file)
        for j in range(subs):
            sub = create_temp_dir(dir)
            path = os.path.join(root, dir)
            os.chdir(path)
            file = "file{}.{}".format(i, j)
            create_temp_file(random.randrange(
                1024, (10*1024*1024), 1), file)
            for k in range(files):
                path = os.path.join(root, dir, sub)
                os.chdir(path)
                file = "file{}.{}.{}".format(i, j, k)
                create_temp_file(random.randrange(
                    1024, (10*1024*1024), 1), file)


def clean_up_s3(resource: S3ServiceResource, bucket: str, folder: str) -> None:
    objects_to_delete = resource.meta.client.list_objects(
        Bucket=bucket, Prefix=folder)

    delete_keys = {'Objects': []}  # type: ignore
    delete_keys['Objects'] = [
        {'Key': k} for k in [
            obj['Key'] for obj in objects_to_delete.get(  # type: ignore
                'Contents', [])]]  # type: ignore

    resource.meta.client.delete_objects(
        Bucket=bucket, Delete=delete_keys)  # type: ignore


def clean_up_dir(dir):
    shutil.rmtree(dir)


@pytest.mark.asyncio
async def test_main():
    # Setup
    rand = str(uuid.uuid4().hex[:6])
    Path(source + rand).mkdir(exist_ok=True)
    Path(tmp + rand).mkdir(exist_ok=True)
    status_file = "status.json"
    create_dir_structure(source + rand, 3, 2, 2)

    # Test
    await s3.main(source + rand, tmp + rand)

    # Verify
    files = s3.check_status(status_file)
    for file, sha256 in files.items():
        response = await s3.get_object_sha256(
            resource, bucket_name, file)
        assert (response.get("ResponseMetadata").get(
                "HTTPStatusCode") == 200)
        assert (response.get("ChecksumSHA256") == sha256)

    # Cleanup
    os.chdir(pwd)
    clean_up_dir(source + rand)
    clean_up_dir(tmp + rand)
    clean_up_s3(resource, bucket_name, "/")
    os.remove(status_file)


class TestUpload:
    @pytest.mark.asyncio
    async def test_upload(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source + rand).mkdir(exist_ok=True)
        Path(tmp + rand).mkdir(exist_ok=True)
        create_dir_structure(source + rand, 1, 1, 1)
        files = s3.get_local_files(source + rand)

        for file in files.keys():
            filename = os.path.basename(file)
            cmpfile = os.path.join(tmp + rand, filename)
            sha256 = await s3.hash(file)

            # Test
            await s3.upload(resource, bucket_name, file, sha256)

            # Verify
            response = download(resource, bucket_name, tmp + rand, file)
            cmpsha = await s3.hash(cmpfile)

            assert (response.get("ResponseMetadata").get(
                "HTTPStatusCode") == 200)
            assert (response.get("ChecksumSHA256") == sha256)
            assert (sha256 == cmpsha)

        # Cleanup
        os.chdir(pwd)
        clean_up_dir(source + rand)
        clean_up_dir(tmp + rand)
        clean_up_s3(resource, bucket_name, "/")

    @pytest.mark.asyncio
    async def test_upload_exists(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        Path(tmp+rand).mkdir(exist_ok=True)
        create_dir_structure(source+rand, 1, 1, 1)
        files: dict[str, str] = s3.get_local_files(source+rand)

        for file in files.keys():
            sha256 = await s3.hash(file)
            await s3.upload(resource, bucket_name, file, sha256)

            # Test
            with pytest.raises(FileExistsError):
                await s3.upload(resource, bucket_name, file, sha256)

        # Cleanup
        os.chdir(pwd)
        clean_up_dir(source + rand)
        clean_up_dir(tmp + rand)
        clean_up_s3(resource, bucket_name, "/")


class TestGetObjectSha:

    @pytest.mark.asyncio
    async def test_get_object_sha256(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        create_dir_structure(source+rand, 1, 1, 1)

        files: dict[str, str] = s3.get_local_files(source+rand)

        for file in files:
            sha256 = await s3.hash(file)
            await s3.upload(resource, bucket_name, file, sha256)

            # Test
            head_object = await s3.get_object_sha256(
                resource, bucket_name, file)

            # Verify
            assert (head_object is not None)
            assert (head_object.get("ChecksumSHA256") == sha256)

        # Cleanup
        os.chdir(pwd)
        clean_up_dir(source+rand)
        clean_up_s3(resource, bucket_name, "/")

    @pytest.mark.asyncio
    async def test_get_object_sha256_no_file(self):
        # Test
        with pytest.raises(exceptions.ClientError):
            await s3.get_object_sha256(
                resource, bucket_name, "not_a_file")


def test_get_local_files():
    # Setup
    rand = str(uuid.uuid4().hex[:6])
    Path(source+rand).mkdir(exist_ok=True)
    create_dir_structure(source+rand, 2, 3, 2)

    # Test
    got_files: dict[str, str] = s3.get_local_files(source+rand)

    want_files: dict[str, str] = {}
    for root, dirs, files in os.walk(source+rand):
        for name in files:
            want_files[os.path.join(root, name)] = ""

    # Verify
    for file in got_files:
        assert (got_files[file] == want_files[file])

    # Cleanup
    os.chdir(pwd)
    clean_up_dir(source+rand)


@pytest.mark.asyncio
async def test_set_hash():
    # Setup
    rand = str(uuid.uuid4().hex[:6])
    Path(source+rand).mkdir(exist_ok=True)
    create_dir_structure(source+rand, 2, 3, 2)

    # Test
    got_files: dict[str, str] = s3.get_local_files(source+rand)
    await s3.set_hash(got_files)

    want_files: dict[str, str] = {}
    for root, dirs, files in os.walk(source+rand):
        for name in files:
            file = os.path.join(root, name)
            want_files[file] = await s3.hash(file)

    # Verify
    for file in got_files:
        assert (got_files[file] == want_files[file])

    # Cleanup
    os.chdir(pwd)
    clean_up_dir(source+rand)


@pytest.mark.asyncio
async def test_status():
    # Setup
    rand = str(uuid.uuid4().hex[:6])
    Path(source+rand).mkdir(exist_ok=True)
    create_dir_structure(source+rand, 2, 3, 2)
    status_file = 'status.json'

    got_files: dict[str, str] = s3.get_local_files(source+rand)
    await s3.set_hash(got_files)

    # Test Save Status
    os.chdir(pwd)
    s3.save_status(got_files, status_file)

    with open(status_file, 'r') as json_file:
        want_files: dict[str, str] = json.load(json_file)

    # Verify
    for file in got_files:
        assert (got_files[file] == want_files[file])

    # Test Check Status
    got_files = s3.check_status(status_file)

    # Verify
    for file in want_files:
        assert (got_files[file] == want_files[file])

    # Cleanup
    os.chdir(pwd)
    os.remove(status_file)
    clean_up_dir(source+rand)
