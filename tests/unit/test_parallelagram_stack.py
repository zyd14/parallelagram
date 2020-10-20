import json
import pytest

from aws_cdk import core
from parallelagram.parallelagram_stack import ParallelagramStack


def get_template():
    app = core.App()
    ParallelagramStack(app, "parallelagram")
    return json.dumps(app.synth().get_stack("parallelagram").template)


def test_sqs_queue_created():
    assert("AWS::SQS::Queue" in get_template())


def test_sns_topic_created():
    assert("AWS::SNS::Topic" in get_template())
