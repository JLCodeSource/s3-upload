import boto3
import uuid
import os
import hashlib
import base64
import random

BUF_SIZE = 65536
s3 = boto3.resource('s3')
bucket_name = 'gb-upload'
bucket = s3.Bucket(name=bucket_name)


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


def upload(resource, bucket, file, sha256):
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


def download(resource, bucket, file):
    resource.Object(bucket, file).download_file(
        f'{file}')


def delete(resource, bucket, file):
    resource.Object(bucket, file).delete()
    os.remove(file)


def hash(file):
    sha256 = hashlib.sha256()
    with open(file, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)
    return base64.b64encode(sha256.digest()).decode()


def process_file(file):
    sha256 = hash(file)
    print(sha256)
    print(file)

    upload(s3, bucket_name, file, sha256)
    download(s3, bucket_name, file)
    # delete(s3, bucket_name, file)


def walk(path):
    file_list = []
    for root, dirs, files in os.walk(path):
        for name in files:
            file_list.append(os.path.join(root, name))
    return file_list


pwd = os.getcwd()
path = os.path.join(pwd, 'tmp')
create_dir_structure(path, 3, 2, 5)
files = walk(path)

for file in files:
    process_file(file)
