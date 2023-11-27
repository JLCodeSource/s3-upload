import shutil
import uuid
import os
import random
import boto3
from pathlib import Path

from mypy_boto3_s3 import S3ServiceResource
from s3_upload import s3


resource = boto3.resource('s3')
bucket_name = 'gb-upload'


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


def test_main():
    # Setup
    pwd = os.getcwd()
    source = os.path.join(pwd, 'source')
    Path(source).mkdir(exist_ok=True)
    tmp = os.path.join(pwd, 'tmp')
    Path(tmp).mkdir(exist_ok=True)
    create_dir_structure(source, 3, 2, 5)

    # Test
    s3.main(source, tmp)

    # Verify
    assert True

    # Cleanup
    os.chdir(pwd)
    clean_up_dir(source)
    clean_up_dir(tmp)
    clean_up_s3(resource, bucket_name, "/")


def test_upload():
    # Setup
    pwd = os.getcwd()
    source = os.path.join(pwd, 'source')
    Path(source).mkdir(exist_ok=True)
    target = os.path.join(pwd, 'tmp')
    Path(target).mkdir(exist_ok=True)
    create_dir_structure(source, 1, 1, 1)
    files = s3.get_local_files(source)

    for file in files.keys():
        filename = os.path.basename(file)
        cmpfile = os.path.join(target, filename)
        sha256 = s3.hash(file)

        # Test
        s3.upload(resource, bucket_name, file, sha256)

        # Verify
        response = s3.download(resource, bucket_name, target, file)
        cmpsha = s3.hash(cmpfile)

        assert (response.get("ResponseMetadata").get("HTTPStatusCode") == 200)
        assert (response.get("ChecksumSHA256") == sha256)
        assert (sha256 == cmpsha)

    # Cleanup
    os.chdir(pwd)
    clean_up_dir(source)
    clean_up_dir(target)
    clean_up_s3(resource, bucket_name, "/")


def test_get_object_sha256():
    # Setup
    pwd: str = os.getcwd()
    source: str = os.path.join(pwd, 'source')
    Path(source).mkdir(exist_ok=True)
    create_dir_structure(source, 1, 1, 1)
    files: dict[str, str] = s3.get_local_files(source)

    for file in files:
        sha256 = s3.hash(file)
        s3.upload(resource, bucket_name, file, sha256)

        # Test
        headObj = s3.getObjectSha(resource, bucket_name, file)

        # Verify
        assert (headObj.get("ChecksumSHA256") == sha256)

    # Cleanup
    os.chdir(pwd)
    clean_up_dir(source)
    clean_up_s3(resource, bucket_name, "/")


def test_get_local_files():
    # Setup
    pwd = os.getcwd()
    source = os.path.join(pwd, 'source')
    Path(source).mkdir(exist_ok=True)
    create_dir_structure(source, 2, 3, 2)

    # Test
    got_files: dict[str, str] = s3.get_local_files(source)

    want_files: dict[str, str] = {}
    for root, dirs, files in os.walk(source):
        for name in files:
            want_files[os.path.join(root, name)] = ""

    # Verify
    for file in got_files:
        assert (got_files[file] == want_files[file])

    # Cleanup
    os.chdir(pwd)
    clean_up_dir(source)
