import json
import logging
import os
from typing import Union
import uuid

import boto3

s3_client = boto3.client("s3")
REQUEST_S3_BUCKET = os.getenv("REQUEST_S3_BUCKET", "sg-phil-testing")


def create_logger() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel("INFO")
    sh = logging.StreamHandler()
    sh.setLevel("INFO")
    logger.addHandler(sh)
    return logger


LOGGER = create_logger()


def prep_s3_object(args: Union[tuple, list] = None, kwargs: dict = None, key: str = ""):
    """ Create an object in S3 which holds positional and keyword arguments to be unpacked by a Lambda worker later"""
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}
    if not key:
        key = str(uuid.uuid4())

    s3_client.put_object(
        Bucket=REQUEST_S3_BUCKET,
        Body=bytes(json.dumps({"args": args, "kwargs": kwargs}).encode("utf-8")),
        Key=key,
    )
    return key


def get_s3_response(response: dict) -> dict:
    """ Retrieve response from worker Lambda which stored its response in S3"""
    s3_bucket = response.get("s3_bucket")
    s3_key = response.get("s3_key")
    LOGGER.info(f"Retrieving data from s3://{s3_bucket}/{s3_key}")
    return json.loads(
        s3_client.get_object(Bucket=s3_bucket, Key=s3_key)["Body"]
        .read()
        .decode("utf-8")
    )
