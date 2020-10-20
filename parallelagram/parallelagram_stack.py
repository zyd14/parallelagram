import os

from aws_cdk import (
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    core
)
import boto3

from parallelagram.config_parser import read_config


class LambdaStack(core.Stack):

    def __init__(self,
                 scope: core.Stack,
                 id: str,
                 **kwargs):
        super().__init__(scope, id)

        self.make_lambda_stack()

    def load_code(self, code_path: str):
        if code_path.startswith('s3://'):
            s3 = boto3.client('s3')
            path = code_path.split('://')[1]
            bucket = path.split('/')[0]
            key = '/'.join(path.split('/')[1:])
            handler_code = s3.get_object(Bucket=bucket,
                                         Key=key)['Body'].read().decode('utf-8')
        else:
            with open(code_path, 'r') as code_in:
                handler_code = code_in.read()
        return handler_code

    def make_lambda_stack(self):
        config = read_config('parallel-config.json')

        lambda_list = []
        for l in config.lambdas:
            lambda_.Function(
                self, l.lambda_name,
                code=lambda_.Code.asset(l.code_path),
                handler=l.lambda_handler,
                timeout=core.Duration.seconds(l.timeout),
                runtime=lambda_.Runtime.PYTHON_3_8
            )
        return lambda_list
