import json
import os
from typing import Dict

from parallelagram.exceptions import ConfigurationException

ASYNC_RESPONSE_TABLE = 'phils_done_tasks'
LAMBDA_ASYNC_PAYLOAD_LIMIT = 256000

SNS_TOPIC = os.getenv('RESULT_UPDATE_TOPIC', 'test')

if not SNS_TOPIC:
    config_error_msg = 'No SNS topic configured for sending result updates'
    raise ConfigurationException(config_error_msg)

