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
                 app: core.App,
                 id: str,
                 code_path: str,
                 function_id: str,
                 handler: str,
                 timeout: int,
                 **kwargs):
        super().__init__(app, id)

        lambda_fn = lambda_.Function(
            self, function_id,
            code=self.load_code(code_path),
            handler=handler,
            timeout=core.Duration.seconds(timeout),
            runtime=lambda_.Runtime.PYTHON_3_8
        )

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


class LambdaFactory:

    @classmethod
    def make_lambda_stack(cls):
        config = read_config('../parallel-config.json')

        lambda_list = []
        for l in config.lambdas:
            lambda_list.append(LambdaStack(app,
                                           id=config.app_name,
                                           code_path=l.code_path,
                                           function_id=l.lambda_name,
                                           handler=l.lambda_handler,
                                           timeout=l.timeout))
        return lambda_list


app = core.App()
lambda_list = LambdaFactory.make_lambda_stack()
app.synth()
