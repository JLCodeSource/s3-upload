import asyncio
import base64
import hashlib
import uuid
import os
import json
from pathlib import Path

from aiobotocore.session import (
    get_session, AioSession)
from types_aiobotocore_s3.client import S3Client
from botocore import exceptions
import pytest

import tests.test_helpers as test_helpers
from s3_upload import s3

MIN_FILE_SIZE = 1024
MAX_FILE_SIZE = (10*1024*1024)
CHUNK_SIZE = 65536
bucket_name = 'gb-upload'


# Test settings
pwd: str = os.getcwd()
source: str = os.path.join(pwd, 'source')

class TestMain:
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_main(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source + rand).mkdir(exist_ok=True)
        status_file: str = "status"+rand+".json"
        test_helpers.create_dir_structure(source + rand, 3, 2, 2)
        os.chdir(pwd)

        # Test
        await s3.main(source + rand, status_file, MAX_FILE_SIZE)

        session: AioSession = get_session()
        async with session.create_client('s3') as client:
            # Verify
            files = s3.load_status(status_file)
            for file, done in files.items():
                response = await s3.get_object_sha256(
                    client, bucket_name, file)
                assert (response.get("ResponseMetadata").get(
                        "HTTPStatusCode") == 200)
                assert (done == "Done")

        # Cleanup
        os.chdir(pwd)
        test_helpers.clean_up_dir(source + rand)
        await test_helpers.clean_up_s3(client, bucket_name, "/")
        os.remove(status_file)

    @pytest.mark.asyncio
    async def test_main_staus_file(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source + rand).mkdir(exist_ok=True)
        status_file: str = "status"+rand+".json"
        test_helpers.create_dir_structure(source + rand, 1, 1, 6)
        os.chdir(pwd)

        files: dict[str, str] = s3.get_local_files(
            source + rand, MAX_FILE_SIZE)
        
        # Set 1/4 hash, 1/4 "", 1/4 Suspect, 1/4 Done 
        counter: int = 0
        for file in files.keys():
            if counter % 4 == 0:
                files[file] = await s3.hash(file)
            elif counter % 4 == 1:
                counter = counter + 1
                continue
            elif counter % 4 == 2:
                files[file] = "Suspect"
            else:
                files[file] = "Done"
            counter = counter + 1

        s3.save_status(files, status_file)

        # Test
        await s3.main(source + rand, status_file, MAX_FILE_SIZE)

        session: AioSession = get_session()
        async with session.create_client('s3') as client:
            # Verify
            files = s3.load_status(status_file)
            counter = 0
            for file, status in files.items():
                if counter % 4 == 2:
                    assert (status == "Suspect")
                    counter = counter + 1
                    continue
                if counter % 4 == 3:
                    assert (status == "Done")
                    counter = counter + 1
                    continue
                response = await s3.get_object_sha256(
                    client, bucket_name, file)
                assert (response.get("ResponseMetadata").get(
                        "HTTPStatusCode") == 200)
                assert (status == "Done")
                counter = counter + 1

        # Cleanup
        os.chdir(pwd)
        test_helpers.clean_up_dir(source + rand)
        await test_helpers.clean_up_s3(client, bucket_name, "/")
        os.remove(status_file)

class TestUpload:
    @pytest.mark.asyncio
    async def test_upload(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source + rand).mkdir(exist_ok=True)
        # Path(tmp + rand).mkdir(exist_ok=True)
        test_helpers.create_dir_structure(source + rand, 1, 1, 1)
        files: dict[str, str] = s3.get_local_files(
            source + rand, MAX_FILE_SIZE)

        session: AioSession = get_session()
        async with session.create_client('s3') as client:
            for file in files.keys():
                sha256: str = await s3.hash(file)

                # Test
                await s3.upload(client, bucket_name, file, sha256)

                # Verify
                response = await s3.get_object_sha256(
                    client, bucket_name, file)

                assert (response.get("ResponseMetadata").get(
                    "HTTPStatusCode") == 200)
                assert (response.get("ChecksumSHA256") == sha256)

                # TODO: get download working async
                # response = await download(client,
                #                          bucket_name, tmp + rand, file)
                # os.chdir(tmp + rand)
                # cmphash: str = await s3.hash(file)

                # assert (response.get("ResponseMetadata").get(
                #        "HTTPStatusCode") == 200)
                # assert (response.get("ChecksumSHA256") == sha256)
                # assert (cmphash == sha256)

        # Cleanup
        os.chdir(pwd)
        test_helpers.clean_up_dir(source + rand)
        # clean_up_dir(tmp + rand)
        await test_helpers.clean_up_s3(client, bucket_name, "/")

    @pytest.mark.asyncio
    async def test_upload_exists(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        test_helpers.create_dir_structure(source+rand, 1, 1, 1)
        files: dict[str, str] = s3.get_local_files(
            source+rand, MAX_FILE_SIZE)

        session: AioSession = get_session()
        async with session.create_client('s3') as client:

            for file in files.keys():
                sha256: str = await s3.hash(file)
                await s3.upload(client, bucket_name, file, sha256)

                # Test
                with pytest.raises(FileExistsError):
                    await s3.upload(client, bucket_name, file, sha256)

        # Cleanup
        os.chdir(pwd)
        test_helpers.clean_up_dir(source + rand)
        await test_helpers.clean_up_s3(client, bucket_name, "/")


    @pytest.mark.asyncio
    async def test_upload_suspects(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        test_helpers.create_dir_structure(source+rand, 1, 1, 1)
        files: dict[str, str] = s3.get_local_files(
            source+rand, MAX_FILE_SIZE)

        for file in files.keys():
            files[file] = "Suspect"

        session: AioSession = get_session()
        async with session.create_client('s3') as client:
            # Test
            for file in files.keys():
                sha256: str = "Suspect"
                await s3.upload(client, bucket_name, file, sha256)

        # Verify
        for file in files.values():
            assert(file == "Suspect")

        # Cleanup
        os.chdir(pwd)
        test_helpers.clean_up_dir(source + rand)
        #await clean_up_s3(client, bucket_name, "/")


class TestGetObjectSha:
    @pytest.mark.asyncio
    async def test_get_object_sha256(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        test_helpers.create_dir_structure(source+rand, 1, 1, 1)

        files: dict[str, str] = s3.get_local_files(
            source+rand, MAX_FILE_SIZE)

        session: AioSession = get_session()
        async with session.create_client('s3') as client:

            for file in files:
                sha256: str = await s3.hash(file)
                await s3.upload(client, bucket_name, file, sha256)

                # Test
                head_object = await s3.get_object_sha256(
                    client, bucket_name, file)

                # Verify
                assert (head_object is not None)
                assert (head_object.get("ChecksumSHA256") == sha256)

        # Cleanup
        os.chdir(pwd)
        test_helpers.clean_up_dir(source+rand)
        await test_helpers.clean_up_s3(client, bucket_name, "/")

    @pytest.mark.asyncio
    async def test_get_object_sha256_no_file(self):
        session: AioSession = get_session()
        async with session.create_client('s3') as client:
            # Test
            with pytest.raises(exceptions.ClientError):
                await s3.get_object_sha256(client, bucket_name, "not_a_file")

class TestGetLocalFiles:
    def test_get_local_files(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        test_helpers.create_dir_structure(source+rand, 2, 3, 2)

        # Test
        got_files: dict[str, str] = s3.get_local_files(
            source+rand, MAX_FILE_SIZE)

        want_files: dict[str, str] = {}
        for root, _, files in os.walk(source+rand):
            for name in files:
                want_files[os.path.join(root, name)] = ""

        # Verify
        for file in got_files:
            assert (got_files[file] == want_files[file])

        # Cleanup
        os.chdir(pwd)
        test_helpers.clean_up_dir(source+rand)

    def test_get_local_files_max_size(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        test_helpers.create_dir_structure(source+rand, 2, 3, 2)
        max_size: int = round(MAX_FILE_SIZE/2)

        # Test
        all_files: dict[str, str] = s3.get_local_files(
            source+rand, MAX_FILE_SIZE)
        got_files: dict[str, str] = s3.get_local_files(
            source+rand, max_size)

        want_files: dict[str, str] = {}
        for root, _, files in os.walk(source+rand):
            for name in files:
                fullpath = os.path.join(root, name)
                if os.stat(fullpath).st_size > max_size:
                    continue
                want_files[fullpath] = ""

        # Verify
        assert (len(all_files) > len(got_files))
        for file in got_files:
            assert (got_files[file] == want_files[file])

        # Cleanup
        os.chdir(pwd)
        test_helpers.clean_up_dir(source+rand)

class TestHash:
    @pytest.mark.asyncio
    async def test_hash_success(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        test_helpers.create_dir_structure(source+rand, 1, 1, 1)
        files: dict[str, str] = s3.get_local_files(
            source+rand, MAX_FILE_SIZE)

        # Test
        file: str = list(files.keys())[0]
        got_hash: str = await s3.hash(file)
        
        # Verify
        sha256: hashlib._Hash = hashlib.sha256()
        with open(file, 'rb') as f:
            while True:
                data: bytes = f.read(s3.BUF_SIZE)
                if not data:
                    break
                sha256.update(data)
        want_hash: str = base64.b64encode(sha256.digest()).decode()
        
        assert(got_hash == want_hash)
        
        os.chdir(pwd)
        test_helpers.clean_up_dir(source+rand)

    @pytest.mark.asyncio
    async def test_hash_os_error(self, monkeypatch: pytest.MonkeyPatch):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        test_helpers.create_dir_structure(source+rand, 2, 2, 2)
        files: dict[str, str] = s3.get_local_files(
            source+rand, MAX_FILE_SIZE)
        file: str = list(files.keys())[0]
        
        # mock
        def mock_sha256():
            raise OSError
        
        monkeypatch.setattr(hashlib, "sha256", mock_sha256)

        # Test
        with pytest.raises(OSError):
            await s3.hash(file)




class TestSetHash:
    @pytest.mark.asyncio
    async def test_set_hash_success(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        test_helpers.create_dir_structure(source+rand, 2, 3, 2)
        status_file = "status" + rand + ".json"

        # Test
        got_files: dict[str, str] = s3.get_local_files(
            source+rand, MAX_FILE_SIZE)
        await s3.set_hash(got_files, status_file)

        want_files: dict[str, str] = {}
        for root, _, files in os.walk(source+rand):
            for name in files:
                file: str = os.path.join(root, name)
                want_files[file] = await s3.hash(file)

        # Verify
        for file in got_files:
            assert (got_files[file] == want_files[file])

        # Cleanup
        os.chdir(pwd)
        test_helpers.clean_up_dir(source+rand)


    @pytest.mark.asyncio
    async def test_set_hash_os_error(self, monkeypatch: pytest.MonkeyPatch):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        test_helpers.create_dir_structure(source+rand, 2, 2, 2)
        status_file: str = "status" + rand + ".json"

        files: dict[str, str] = s3.get_local_files(
            source+rand, MAX_FILE_SIZE)

        # mock
        async def mock_hash(file):
            raise OSError

        monkeypatch.setattr(s3, "hash", mock_hash)

        # Test
        await s3.set_hash(files, status_file)
        # Verify
        for file in files.values():
            assert(file == "Suspect")

        # Cleanup
        os.chdir(pwd)
        test_helpers.clean_up_dir(source+rand)


@pytest.mark.asyncio
async def test_status():
    # Setup
    rand = str(uuid.uuid4().hex[:6])
    Path(source+rand).mkdir(exist_ok=True)
    test_helpers.create_dir_structure(source+rand, 2, 3, 2)
    status_file: str = 'status'+rand+'.json'

    got_files: dict[str, str] = s3.get_local_files(
        source+rand, MAX_FILE_SIZE)
    await s3.set_hash(got_files, status_file)

    # Test Save Status
    os.chdir(pwd)
    s3.save_status(got_files, status_file)

    with open(status_file, 'r') as json_file:
        want_files: dict[str, str] = json.load(json_file)

    # Verify
    for file in got_files:
        assert (got_files[file] == want_files[file])

    # Test Load Status
    got_files = s3.load_status(status_file)

    # Verify
    for file in want_files:
        assert (got_files[file] == want_files[file])

    # Cleanup
    os.chdir(pwd)
    os.remove(status_file)
    test_helpers.clean_up_dir(source+rand)


class TestCheckStatus:
    def test_check_status_success(self):
        # Setup
        rand = str(uuid.uuid4().hex[:6])
        Path(source+rand).mkdir(exist_ok=True)
        test_helpers.create_dir_structure(source+rand, 2, 3, 2)
        status_file: str = 'status'+rand+'.json'

        want_files: dict[str, str] = s3.get_local_files(
            source+rand, MAX_FILE_SIZE)

        os.chdir(pwd)
        s3.save_status(want_files, status_file)

        # Test Check Status
        got_files: dict[str, str] = s3.check_status(
            source+rand, status_file, MAX_FILE_SIZE)

        # Verify

        for file in got_files:
            assert (got_files[file] == want_files[file])

        # Cleanup
        os.chdir(pwd)
        os.remove(status_file)
        test_helpers.clean_up_dir(source+rand)

    def test_check_status_FileNotFound(self):
        pass


@pytest.mark.asyncio
async def test_add_files_to_queues():
    # Setup
    rand = str(uuid.uuid4().hex[:6])
    Path(source+rand).mkdir(exist_ok=True)
    test_helpers.create_dir_structure(source+rand, 2, 3, 2)

    hash_q: asyncio.Queue[str] = asyncio.Queue()
    upload_q: asyncio.Queue[str] = asyncio.Queue()
    got_files: dict[str, str] = s3.get_local_files(
        source+rand, MAX_FILE_SIZE)

    counter: int = 0
    # Make the values for 1/4th of the files(keys) equal
    # to a '' & 1/4th "Suspect" (i.e. both continue), 
    # 1/4th a random uuid string & 1/4th, Done.
    # This way we can check the add_files_to_queues logic
    for file, _ in got_files.items():
        if counter % 4 == 0:
            got_files[file] = str(uuid.uuid4())
        elif counter % 4 == 1:
            counter = counter + 1
            continue
        elif counter % 4 == 2:
            got_files[file] = "Suspect"
        else:
            got_files[file] = "Done"
        counter = counter + 1
    # Test
    session: AioSession = get_session()
    async with session.create_client('s3') as _:
        await s3.add_files_to_queues(
            got_files, hash_q, upload_q)

    # Verify
    # N.B. Dirty test to check that 1/4th are in hash_q,
    # 1/4th are in upload_q & 1/4th are Done & Suspect
    assert (hash_q.qsize() == 5)
    assert (upload_q.qsize() == 5)
    assert (sum(v == "Done" for v in got_files.values()) == 5)
    assert (sum(v == "Suspect" for v in got_files.values()) == 5) 

    # Cleanup
    os.chdir(pwd)
    test_helpers.clean_up_dir(source+rand)
