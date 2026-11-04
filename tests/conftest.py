"""Run the S3 tests against a local moto server instead of real AWS.

moto implements the S3 API in-process, so no credentials, network access or
AWS account are needed. aiobotocore and boto3 both honour AWS_ENDPOINT_URL,
so the production code needs no changes.
"""

import os
from collections.abc import Iterator

import boto3
import pytest
from moto.server import ThreadedMotoServer

BUCKET = "gb-upload"
HOST = "127.0.0.1"
PORT = 5555


@pytest.fixture(scope="session", autouse=True)
def moto_s3() -> Iterator[str]:
    server = ThreadedMotoServer(ip_address=HOST, port=PORT, verbose=False)
    server.start()
    endpoint = f"http://{HOST}:{PORT}"
    overrides = {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }
    previous = {key: os.environ.get(key) for key in overrides}
    os.environ.update(overrides)
    try:
        boto3.client("s3").create_bucket(Bucket=BUCKET)
        yield endpoint
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        server.stop()
