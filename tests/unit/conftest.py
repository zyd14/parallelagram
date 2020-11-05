from uuid import uuid4

import attr
import pytest

from parallelagram.launchables import Lambdable

good_config = {
              "app_name": "testarino",
              "lambdas": [
                  {
                    "lambda_handler": "test_handler",
                    "runtime": "python3.8",
                    "code_path": "example_handler.py",
                    "lambda_name": "example-lambda",
                    "s3_request_bucket": "sg-phil-testing",
                    "s3_response_bucket": "sg-phil-testing",
                    "dynamo_response_table": "phils_done_tasks"
                  }
                ]
            }

bad_config = {
              "lambdas": [
                  {
                    "s3_response_bucket": "sg-phil-testing",
                    "dynamo_response_table": "phils_done_tasks"
                  }
                ]
            }


@pytest.fixture(scope='module')
def mock_config():
    return good_config


@pytest.fixture(scope='module')
def mock_bad_config():
    return bad_config


@attr.s
class MockAsyncResponse:
    response_id = attr.ib(default=str(uuid4()))  # type: str


@pytest.fixture(scope='module')
def mock_async_response():
    return MockAsyncResponse()


@pytest.fixture
def mock_base_lambdable():
    return Lambdable(func_path='some.func.path',
                     remote_aws_lambda_func_name='some-lambda-func',
                     args=[],
                     kwargs={})
