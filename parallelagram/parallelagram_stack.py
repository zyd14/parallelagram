from typing import List, Type

from aws_cdk import (
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    core
)
import boto3

from parallelagram.config_parser import read_config, ParallelagramConfig


class LambdaStack(core.Stack):

    def __init__(self,
                 scope: core.Stack,
                 id: str,
                 **kwargs):
        super().__init__(scope, id)

        config = read_config('parallel-config.json')
        self.make_lambda_stack(config)


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

    def make_lambda_stack(self, config: ParallelagramConfig) -> List[Type[core.Stack]]:

        lambda_list = []
        existing_tables = set()
        for i, l in enumerate(config.lambdas):
            table_name = f'table_{i}'
            if table_name not in existing_tables:
                existing_tables.add(table_name)
                table = dynamodb.Table(self, table_name,
                                       partition_key=dynamodb.Attribute(name='response_id',
                                                                        type=dynamodb.AttributeType.STRING),
                                       read_capacity=l.response_table_read_capacity,
                                       write_capacity=l.response_table_write_capacity,
                                       time_to_live_attribute='ttl')
            lambda_.Function(
                self, l.lambda_name,
                code=lambda_.Code.asset(l.code_path),
                handler=l.lambda_handler,
                timeout=core.Duration.seconds(l.timeout),
                runtime=lambda_.Runtime.PYTHON_3_8
            )
        return lambda_list
