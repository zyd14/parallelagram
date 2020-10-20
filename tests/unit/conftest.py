import pytest

good_config = {
              "lambdas": [
                  {
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