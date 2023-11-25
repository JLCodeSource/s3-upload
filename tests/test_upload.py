import shutil
import uuid
import os
import random
import boto3
from pathlib import Path
from s3_upload import upload


s3 = boto3.resource('s3')
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
    upload.main(source, tmp)

    # Verify
    assert True

    os.chdir(pwd)
    clean_up_dir(source)
    clean_up_dir(tmp)


def test_upload():
    # Setup
    pwd = os.getcwd()
    source = os.path.join(pwd, 'source')
    Path(source).mkdir(exist_ok=True)
    target = os.path.join(pwd, 'tmp')
    Path(target).mkdir(exist_ok=True)
    create_dir_structure(source, 1, 1, 1)
    files = upload.walk(source)
    filename = os.path.basename(files[0])
    cmpfile = os.path.join(target, filename)
    sha256 = upload.hash(files[0])

    # Test
    upload.upload(s3, bucket_name, files[0], sha256)

    # Verify
    response = upload.download(s3, bucket_name, target, files[0])
    cmpsha = upload.hash(cmpfile)

    assert (response.get("ResponseMetadata").get("HTTPStatusCode") == 200)
    assert (response.get("ChecksumSHA256") == sha256)
    assert (sha256 == cmpsha)

    os.chdir(pwd)
    clean_up_dir(source)
    clean_up_dir(target)


def test_get_object_sha256():
    # Setup
    pwd = os.getcwd()
    source = os.path.join(pwd, 'source')
    Path(source).mkdir(exist_ok=True)
    create_dir_structure(source, 1, 1, 1)
    files = upload.walk(source)
    sha256 = upload.hash(files[0])
    upload.upload(s3, bucket_name, files[0], sha256)

    # Test
    headObj = upload.getObjSha(s3, bucket_name, files[0])

    assert (headObj.get("ChecksumSHA256") == sha256)

    os.chdir(pwd)
    clean_up_dir(source)
