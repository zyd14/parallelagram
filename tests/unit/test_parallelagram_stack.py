import json

import pytest

from aws_cdk import core
from parallelagram.parallelagram_stack import LambdaStack


def get_template():
    app = core.App()
    LambdaStack(app, "parallelagram")
    return json.dumps(app.synth().get_stack("parallelagram").template)


@pytest.mark.skip
def test_sqs_queue_created():
    template = get_template()
    assert "AWS::DynamoDB::Table" in template

