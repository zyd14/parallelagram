import json
import os

from marshmallow import ValidationError
import pytest

from parallelagram.config_parser import read_config


class TestReadConfig:

    test_filepath = 'test-config.json'
    test_bad_config = 'test-bad-config.json'

    def setup_class(self):
        from tests.unit.conftest import good_config, bad_config
        with open(self.test_filepath, 'w') as out:
            json.dump(good_config, out)

        with open(self.test_bad_config, 'w') as out:
            json.dump(bad_config, out)

    @staticmethod
    def remove_file(path: str):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def teardown_class(self):
        self.remove_file(self.test_filepath)
        self.remove_file(self.test_bad_config)

    def test_read(self, mock_config):
        data = read_config(self.test_filepath)
        assert len(data.lambdas) == 1
        lambdas = mock_config.get('lambdas')[0]
        assert data.lambdas[0].code_path == lambdas.get('code_path')
        assert data.lambdas[0].dynamo_response_table == lambdas.get('dynamo_response_table')
        assert data.lambdas[0].lambda_name == lambdas.get('lambda_name')
        assert data.lambdas[0].s3_request_bucket == lambdas.get('s3_request_bucket')
        assert data.lambdas[0].s3_response_bucket == lambdas.get('s3_response_bucket')

    def test_throws_validation_error(self):
        with pytest.raises(ValidationError):
            read_config(self.test_bad_config)
