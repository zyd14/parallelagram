import json
from typing import List

import attr
from marshmallow import Schema, fields, post_load, ValidationError


@attr.s
class RemoteLambda:
    code_path = attr.ib()  # type: str
    lambda_name = attr.ib()  # type: str
    s3_request_bucket = attr.ib()  # type: str
    s3_response_bucket = attr.ib()  # type: str
    dynamo_response_table = attr.ib()  # type: str


@attr.s
class ParallelagramConfig:
    lambdas = attr.ib()  # type: List[RemoteLambda]


class RemoteLambdaConfig(Schema):
    code_path = fields.Str(required=True)
    lambda_name = fields.Str(required=True)
    s3_request_bucket = fields.Str(required=True)
    s3_response_bucket = fields.Str(required=True)
    dynamo_response_table = fields.Str(required=True)

    @post_load()
    def loader(self, data, **kwargs):
        return RemoteLambda(**data)


class ParallelagramConfigSchema(Schema):
    lambdas = fields.List(fields.Nested(RemoteLambdaConfig))

    @post_load()
    def loader(self, data, **kwargs):
        return ParallelagramConfig(**data)


def read_config(path: str = '../parallel-config.json') -> ParallelagramConfig:
    with open(path, 'r') as in_conf:
        conf = json.load(in_conf)
    try:
        data = ParallelagramConfigSchema().load(conf)
    except ValidationError as ve:
        print(f'Invalid configuration found at path {path}. Please check the config file and try again.')
        raise ve

    return data
