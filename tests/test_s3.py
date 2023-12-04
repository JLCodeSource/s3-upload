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


class TestFile:
    @pytest.mark.asyncio
    async def test_File(self):
        file = s3.File(name="", is_hashed=False, is_uploaded=False, is_errored=False, sha256="")
        assert(type(file) == s3.File)

class TestMain:
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_main(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : True,
            "dirs" : (3, 2, 2)
        }
        source, status_file = test_helpers.setup(fixtures)

        # Test
        await s3.main(source, str(status_file), MAX_FILE_SIZE)

        session: AioSession = get_session()
        async with session.create_client('s3') as client:
            # Verify
            files = s3.load_status(str(status_file))
            for file, status in files.items():
                response = await s3.get_object_sha256(
                    client, bucket_name, file)
                assert (response.get("ResponseMetadata").get(
                        "HTTPStatusCode") == 200)
                assert (status == "Uploaded")

        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = client
        teardown["status_file"] = status_file
        await test_helpers.teardown(teardown)


    @pytest.mark.asyncio
    async def test_main_staus_file(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : True,
            "dirs" : (1, 1, 6)
        }
        source, status_file = test_helpers.setup(fixtures)

        files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)
        
        # Set 1/4 hash, 1/4 "", 1/4 Suspect, 1/4 Uploaded 
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
                files[file] = "Uploaded"
            counter = counter + 1

        s3.save_status(files, str(status_file))

        # Test
        await s3.main(source, str(status_file), MAX_FILE_SIZE)

        session: AioSession = get_session()
        async with session.create_client('s3') as client:
            # Verify
            files = s3.load_status(str(status_file))
            counter = 0
            for file, status in files.items():
                if counter % 4 == 2:
                    assert (status == "Suspect")
                    counter = counter + 1
                    continue
                if counter % 4 == 3:
                    assert (status == "Uploaded")
                    counter = counter + 1
                    continue
                response = await s3.get_object_sha256(
                    client, bucket_name, file)
                assert (response.get("ResponseMetadata").get(
                        "HTTPStatusCode") == 200)
                assert (status == "Uploaded")
                counter = counter + 1

        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = client
        teardown["status_file"] = status_file
        await test_helpers.teardown(teardown)

class TestUpload:
    @pytest.mark.asyncio
    async def test_upload(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (1, 1, 1)
        }
        source, status_file = test_helpers.setup(fixtures)

        files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)

        session: AioSession = get_session()
        async with session.create_client('s3') as client:
            for file in files.keys():
                sha256: str = await s3.hash(file)

                # Test
                status = await s3.upload(client, bucket_name, file, sha256)

                # Verify
                response = await s3.get_object_sha256(
                    client, bucket_name, file)

                assert (response.get("ResponseMetadata").get(
                    "HTTPStatusCode") == 200)
                assert (response.get("ChecksumSHA256") == sha256)

                assert (status == "Uploaded")

        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = client
        teardown["status_file"] = status_file
        await test_helpers.teardown(teardown)
        
    @pytest.mark.asyncio
    async def test_upload_exists(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (1, 1, 1)
        }
        source, _ = test_helpers.setup(fixtures)
        
        
        files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)

        session: AioSession = get_session()
        async with session.create_client('s3') as client:

            for file in files.keys():
                sha256: str = await s3.hash(file)
                await s3.upload(client, bucket_name, file, sha256)

                # Test
                with pytest.raises(FileExistsError):
                    await s3.upload(client, bucket_name, file, sha256)

        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = client
        teardown["status_file"] = None
        await test_helpers.teardown(teardown)

    @pytest.mark.asyncio
    async def test_upload_file_not_found(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (1, 0, 0)
        }
        source, status_file = test_helpers.setup(fixtures)

        files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)

        session: AioSession = get_session()
        async with session.create_client('s3') as client:
            for file in files.keys():
                sha256: str = await s3.hash(file)

                # remove files
                test_helpers.clean_up_dir(source)

                # Verify
                with pytest.raises(FileNotFoundError):
                # Test
                    await s3.upload(client, bucket_name, file, sha256)

                    
        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = None
        teardown["client"] = None
        teardown["status_file"] = status_file
        await test_helpers.teardown(teardown)
        

    @pytest.mark.asyncio
    async def test_upload_status_suspects(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (1, 1, 1)
        }
        source, _ = test_helpers.setup(fixtures)

        files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)

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
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = None
        teardown["status_file"] = None
        await test_helpers.teardown(teardown)

    @pytest.mark.asyncio
    async def test_upload_filesystem_suspects(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (1, 0, 0)
        }
        source, _ = test_helpers.setup(fixtures)

        files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)

        for file in files.keys():
            os.remove(file)

        session: AioSession = get_session()
        with pytest.raises(FileNotFoundError):
            async with session.create_client('s3') as client:
                # Test
                for file in files.keys():
                    sha256 = "WDE6ZSnSCMhecQYimORcJgZwMeSvGbNO37Svw9ATruo="
                    await s3.upload(client, bucket_name, file, sha256)

        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = None
        teardown["status_file"] = None
        await test_helpers.teardown(teardown)


    @pytest.mark.asyncio
    async def test_upload_sha_mismatch(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (1, 0, 0)
        }
        source, status_file = test_helpers.setup(fixtures)

        files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)

        session: AioSession = get_session()
        async with session.create_client('s3') as client:
            for file in files.keys():
                initial_sha256: str = await s3.hash(file)

                uploaded: str = await s3.upload(client, bucket_name, file, initial_sha256)
                assert (uploaded == "Uploaded")

                # Modify file
                append_bytes = b'\xC3\xA9'
                with open(file, "ab") as f:
                    f.write(append_bytes)

                post_sha256: str = await s3.hash(file)
                #log: str = f"Uploaded hash {initial_sha256} does not match local hash {post_sha256}"

                # Test
                mismatch: str = await s3.upload(client, bucket_name, file, post_sha256)
                
                # Verify
                assert(mismatch == "Mismatch")
                    
        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = None
        teardown["client"] = None
        teardown["status_file"] = status_file
        await test_helpers.teardown(teardown)



class TestGetObjectSha:
    @pytest.mark.asyncio
    async def test_get_object_sha256(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (1, 1, 1)
        }
        source, _ = test_helpers.setup(fixtures)

        files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)

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
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = client
        teardown["status_file"] = None
        await test_helpers.teardown(teardown)

    @pytest.mark.asyncio
    async def test_get_object_sha256_no_file(self):
        session: AioSession = get_session()
        async with session.create_client('s3') as client:
            # Test
            with pytest.raises(exceptions.ClientError):
                await s3.get_object_sha256(client, bucket_name, "not_a_file")

class TestGetLocalFiles:
    @pytest.mark.asyncio
    async def test_get_local_files(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (2, 3, 2)
        }
        source, _ = test_helpers.setup(fixtures)

        # Test
        got_files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)

        want_files: dict[str, str] = {}
        for root, _, files in os.walk(source):
            for name in files:
                want_files[os.path.join(root, name)] = ""

        # Verify
        for file in got_files:
            assert (got_files[file] == want_files[file])

        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = None
        teardown["status_file"] = None
        await test_helpers.teardown(teardown)

    @pytest.mark.asyncio
    async def test_get_local_files_max_size(self):
        # Setup
                # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (1, 2, 2)
        }
        source, _ = test_helpers.setup(fixtures)
        max_size: int = round(MAX_FILE_SIZE/2)

        # Test
        all_files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)
        got_files: dict[str, str] = s3.get_local_files(
            source, max_size)

        want_files: dict[str, str] = {}
        for root, _, files in os.walk(source):
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
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = None
        teardown["status_file"] = None
        await test_helpers.teardown(teardown)


class TestHash:
    @pytest.mark.asyncio
    async def test_hash_success(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (1, 1, 1)
        }
        source, _ = test_helpers.setup(fixtures)

        files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)

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
        
        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = None
        teardown["status_file"] = None
        await test_helpers.teardown(teardown)


    @pytest.mark.asyncio
    async def test_hash_os_error(self, monkeypatch: pytest.MonkeyPatch):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (2, 2, 2)
        }
        source, _ = test_helpers.setup(fixtures)
        
        files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)
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
        fixtures: dict[str, bool | tuple] = {
            "status_file" : True,
            "dirs" : (2, 3, 2)
        }
        source, status_file = test_helpers.setup(fixtures)
        
        # Test
        got_files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)
        await s3.set_hash(got_files, str(status_file))

        want_files: dict[str, str] = {}
        for root, _, files in os.walk(source):
            for name in files:
                file: str = os.path.join(root, name)
                want_files[file] = await s3.hash(file)

        # Verify
        for file in got_files:
            assert (got_files[file] == want_files[file])

        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = None
        teardown["status_file"] = status_file
        await test_helpers.teardown(teardown)



    @pytest.mark.asyncio
    async def test_set_hash_os_error(self, monkeypatch: pytest.MonkeyPatch):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : True,
            "dirs" : (2, 2, 2)
        }
        source, status_file = test_helpers.setup(fixtures)

        files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)

        # mock
        async def mock_hash(file):
            raise OSError

        monkeypatch.setattr(s3, "hash", mock_hash)

        # Test
        await s3.set_hash(files, str(status_file))
        # Verify
        for file in files.values():
            assert(file == "Suspect")

        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = None
        teardown["status_file"] = None
        await test_helpers.teardown(teardown)
        

@pytest.mark.asyncio
async def test_status():
    # Setup
    fixtures: dict[str, bool | tuple] = {
            "status_file" : True,
            "dirs" : (2, 3, 2)
        }
    source, status_file = test_helpers.setup(fixtures)

    got_files: dict[str, str] = s3.get_local_files(
        source, MAX_FILE_SIZE)
    await s3.set_hash(got_files, str(status_file))

    # Test Save Status
    os.chdir(test_helpers.pwd)
    s3.save_status(got_files, str(status_file))

    with open(str(status_file), 'r') as json_file:
        want_files: dict[str, str] = json.load(json_file)

    # Verify
    for file in got_files:
        assert (got_files[file] == want_files[file])

    # Test Load Status
    got_files = s3.load_status(str(status_file))

    # Verify
    for file in want_files:
        assert (got_files[file] == want_files[file])

    # Cleanup
    teardown: dict[str, bool | str | S3Client | None] = {}
    teardown["source"] = source
    teardown["client"] = None
    teardown["status_file"] = status_file
    await test_helpers.teardown(teardown)


class TestCheckStatus:
    @pytest.mark.asyncio
    async def test_check_status_success(self):
        # Setup
        fixtures: dict[str, bool | tuple] = {
            "status_file" : True,
            "dirs" : (2, 3, 2)
        }
        source, status_file = test_helpers.setup(fixtures)

        want_files: dict[str, str] = s3.get_local_files(
            source, MAX_FILE_SIZE)

        os.chdir(test_helpers.pwd)
        s3.save_status(want_files, str(status_file))

        # Test Check Status
        got_files: dict[str, str] = s3.check_status(
            source, status_file, MAX_FILE_SIZE)

        # Verify

        for file in got_files:
            assert (got_files[file] == want_files[file])

        # Cleanup
        teardown: dict[str, bool | str | S3Client | None] = {}
        teardown["source"] = source
        teardown["client"] = None
        teardown["status_file"] = status_file
        await test_helpers.teardown(teardown)

    # TODO: Need to add this test!
    def test_check_status_FileNotFound(self):
        pass


@pytest.mark.asyncio
async def test_add_files_to_queues():
    # Setup
    fixtures: dict[str, bool | tuple] = {
            "status_file" : False,
            "dirs" : (2, 3, 2)
        }
    source, _ = test_helpers.setup(fixtures)

    hash_q: asyncio.Queue[str] = asyncio.Queue()
    upload_q: asyncio.Queue[str] = asyncio.Queue()
    got_files: dict[str, str] = s3.get_local_files(
        source, MAX_FILE_SIZE)

    counter: int = 0
    # Make the values for 1/4th of the files(keys) equal
    # to a '' & 1/4th "Suspect" (i.e. both continue), 
    # 1/4th a random uuid string & 1/4th, Uploaded.
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
            got_files[file] = "Uploaded"
        counter = counter + 1
    # Test
    session: AioSession = get_session()
    async with session.create_client('s3') as _:
        await s3.add_files_to_queues(
            got_files, hash_q, upload_q)

    # Verify
    # N.B. Dirty test to check that 1/4th are in hash_q,
    # 1/4th are in upload_q & 1/4th are Uploaded & Suspect
    assert (hash_q.qsize() == 5)
    assert (upload_q.qsize() == 5)
    assert (sum(v == "Uploaded" for v in got_files.values()) == 5)
    assert (sum(v == "Suspect" for v in got_files.values()) == 5) 

    # Cleanup
    teardown: dict[str, bool | str | S3Client | None] = {}
    teardown["source"] = source
    teardown["client"] = None
    teardown["status_file"] = None
    await test_helpers.teardown(teardown)
    