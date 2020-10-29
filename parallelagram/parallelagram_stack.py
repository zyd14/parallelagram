import os

project_dir = os.path.dirname(os.path.realpath(os.path.dirname(__file__)))
config_path = os.path.join(project_dir, 'parallel-config.json')

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

        config = read_config(config_path)
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
        existing_tables = {}
        for i, l in enumerate(config.lambdas):
            table_name = f'table_{i}'
            if table_name not in existing_tables:
                table = dynamodb.Table(self, table_name,
                                       partition_key=dynamodb.Attribute(name='response_id',
                                                                        type=dynamodb.AttributeType.STRING),
                                       read_capacity=l.response_table_read_capacity,
                                       write_capacity=l.response_table_write_capacity,
                                       time_to_live_attribute='ttl')
                existing_tables.update({table_name: table})

            fn = lambda_.Function(self, l.lambda_name,
                                  code=lambda_.Code.asset(l.code_path),
                                  handler=l.lambda_handler,
                                  timeout=core.Duration.seconds(l.timeout),
                                  memory_size=l.memory_size,
                                  runtime=self.get_runtime(l.runtime),
                                  tracing=self.get_trace_value(l.tracing)
                                )
            existing_tables.get(table_name).grant_read_write_data(fn)

        return lambda_list

    def get_trace_value(self, config_trace: bool) -> lambda_.Tracing:
        if config_trace:
            tracing_value = lambda_.Tracing.ACTIVE
        else:
            tracing_value = lambda_.Tracing.DISABLED
        return tracing_value

    def get_runtime(self, config_runtime: str) -> lambda_.Runtime:
        runtimes = {'python3.8': lambda_.Runtime.PYTHON_3_8,
                    'python3.7': lambda_.Runtime.PYTHON_3_7,
                    'python3.6': lambda_.Runtime.PYTHON_3_6,
                    'python2.7': lambda_.Runtime.PYTHON_2_7,
                    'node12.x': lambda_.Runtime.NODEJS_12_X,
                    'node10.x': lambda_.Runtime.NODEJS_10_X,
                    'java11': lambda_.Runtime.JAVA_11,
                    'java8': lambda_.Runtime.JAVA_8,
                    'java8_corretto': lambda_.Runtime.JAVA_8_CORRETTO}
        try:
            return runtimes[config_runtime]
        except KeyError as ke:
            print(f'No lambda runtime exists for configured value {ke.args[0]}.'
                  f'Valid configurable runtimes: {[runtimes.keys()]}')
