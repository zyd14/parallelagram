import importlib
import inspect
import json
import logging
from typing import Union
import uuid

import boto3

from parallelagram.config import ASYNC_RESPONSE_TABLE, AWS_REGION, REQUEST_S3_BUCKET
from parallelagram.exceptions import NoSuchFunctionFound

aws_session = boto3.Session()
s3_client = boto3.client("s3")
dynamo_client = boto3.client("dynamodb", region_name=AWS_REGION)
lambda_client = aws_session.client("lambda", region_name=AWS_REGION)


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

    s3_object = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)["Body"].read().decode("utf-8")
    try:
        return json.loads(s3_object)
    except json.decoder.JSONDecodeError:
        return s3_object


def get_async_response(response_id):
    """
    Get the response from the async table
    """
    response = dynamo_client.get_item(
        TableName=ASYNC_RESPONSE_TABLE, Key={"id": {"S": str(response_id)}}
    )
    if "Item" not in response:
        return None

    return {
        "status": response["Item"]["async_status"]["S"],
        "response": json.loads(response["Item"]["async_response"]["S"]),
    }


LAMBDA_ASYNC_PAYLOAD_LIMIT = 256000


def import_and_get_task(task_path):
    """ Given a modular path to a function, import that module and return the function."""
    module, function = task_path.rsplit('.', 1)
    app_module = importlib.import_module(module)
    try:
        # Return function from module
        return getattr(app_module, function)
    except AttributeError as ae:
        raise NoSuchFunctionFound(task_path=task_path,
                                  exc_string=ae.args[0])


def get_func_task_path(func):
    """
    Format the modular task path for a function via inspection.
    """
    module_path = inspect.getmodule(func).__name__
    task_path = "{module_path}.{func_name}".format(
        module_path=module_path, func_name=func.__name__
    )
    return task_path