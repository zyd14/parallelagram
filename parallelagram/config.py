import os

from parallelagram.exceptions import ConfigurationException

ASYNC_RESPONSE_TABLE = 'phils_done_tasks'
LAMBDA_ASYNC_PAYLOAD_LIMIT = 256000
AWS_REGION = os.getenv('AWS_REGION', 'us-west-2')
SNS_TOPIC = os.getenv('RESULT_UPDATE_TOPIC', 'test')
REQUEST_S3_BUCKET = os.getenv("REQUEST_S3_BUCKET", "sg-phil-testing")

if not SNS_TOPIC:
    config_error_msg = 'No SNS topic configured for sending result updates'
    raise ConfigurationException(config_error_msg)

