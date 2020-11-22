import json
from unittest.mock import patch

import pytest

from parallelagram.remote_handler import put_response_in_s3
from parallelagram.utils import import_and_get_task, prep_s3_object, get_s3_response, REQUEST_S3_BUCKET
from parallelagram.exceptions import NoSuchFunctionFound


class TestImportAndGetTask:

    def test_nominal(self):
        function = import_and_get_task('parallelagram.remote_handler.put_response_in_s3')
        assert function == put_response_in_s3

    def test_function_doesnt_exist(self):

        with pytest.raises(NoSuchFunctionFound):
            import_and_get_task('parallelagram.remote_handler.nonexistent_function')


class TestGetS3Object:

    @patch('parallelagram.utils.s3_client')
    def test_default_args(self, mock_client):
        mock_client.put_object.return_value = None

        result_key = prep_s3_object()
        assert len(result_key) > 10
        assert mock_client.put_object.call_count == 1
        called_args = mock_client.put_object.call_args_list[0].kwargs
        assert called_args.get('Bucket') == REQUEST_S3_BUCKET
        assert called_args.get('Body') == b'{"args": [], "kwargs": {}}'
        assert called_args.get('Key') == result_key


class MockObject:

    def __init__(self, read_value: bytes):
        self.read_value = read_value

    def read(self, *args, **kwargs):
        return self.read_value


class TestGetS3Response:

    @patch('parallelagram.utils.s3_client')
    def test_non_json(self, mock_client):
        test_object_value = b'some non json text'
        mock_client.get_object.return_value = {'Body': MockObject(test_object_value)}

        test_response = {'s3_bucket': 'some_fake_bucket',
                         's3_key': 'some_fake_key'}
        result_object = get_s3_response(test_response)
        assert result_object == test_object_value.decode('utf-8')

    @patch('parallelagram.utils.s3_client')
    def test_json(self, mock_client):
        test_object_value = bytes(json.dumps({'some': 'stuff'}).encode('utf-8'))
        mock_client.get_object.return_value = {'Body': MockObject(test_object_value)}

        test_response = {'s3_bucket': 'some_fake_bucket',
                         's3_key': 'some_fake_key'}
        result_object = get_s3_response(test_response)
        assert result_object == {'some': 'stuff'}
