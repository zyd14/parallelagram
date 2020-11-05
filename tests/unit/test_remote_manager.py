from unittest.mock import patch
from uuid import uuid4

import pytest

from parallelagram.remote_manager import check_for_errors, launch_remote_tasks, try_getting_responses
from tests.unit.conftest import MockAsyncResponse
from utils import prep_s3_object
from launchables import TaskMap
from parallelagram import Lambdable


class MockAsyncResponse:

    def __init__(self, response_id: str = ''):
        if not response_id:
            response_id = uuid4()
        self.response_id = response_id


def mock_run(response_id: str = '', *args, **kwargs):
    if not response_id:
        response_id = str(uuid4())
    return MockAsyncResponse(response_id=response_id)


def mock_return_response_id(*args, **kwargs):
    return uuid4()


def mock_task_map_func(*args, **kwargs):
    return MockAsyncResponse()


def mock_none_return(*args, **kwargs):
    return None


class TestS3Prep:

    def test_nominal(self, monkeypatch):
        def null_return(*args, **kwargs):
            return None
        monkeypatch.setattr('parallelagram.remote_manager.s3_client.put_object', null_return)

        key = prep_s3_object()
        assert len(key) > 20 and isinstance(key, str)


class TestCheckForErrors:

    def test_no_errors(self):

        test_response = [{'response': "here's some successful data"}]
        assert not check_for_errors(test_response)

    def test_has_nas_or_unhandled_exceptions(self):
        test_response = [{'response': 'N/A'}, {'status': 'failed'}, {'UnhandledException': 'some random error'}]
        assert len(test_response) == 3


class TestLaunchRemoteTasks:

    @patch('parallelagram.zappa_async_fork.LambdaAsyncResponse.send')
    def test_lambdable_list(self, mock_send):
        mock_send.return_value = MockAsyncResponse()

        test_lambdable_captured = Lambdable(func_path='some.func.path',
                                            remote_aws_lambda_func_name='some-lambda-func',
                                            args=[],
                                            kwargs={})
        test_lambdable_not_captured = Lambdable(func_path='another.path',
                                                remote_aws_lambda_func_name='another-lambda',
                                                capture_response=False)
        task_list = [test_lambdable_captured, test_lambdable_not_captured]

        response_ids = launch_remote_tasks(task_list)
        assert len(response_ids) == 1

    def test_task_map(self):

        task_map = TaskMap()
        task_map.add_task(remote_task=mock_task_map_func)
        task_map.add_task(remote_task=mock_task_map_func)

        response_ids = launch_remote_tasks(task_map)
        assert len(response_ids) == 2

    def test_bad_type_raises(self):

        task_list = {'not the write stuff'}
        with pytest.raises(TypeError):
            launch_remote_tasks(task_list)


class TestTryGettingResponses:

    @patch('parallelagram.remote_manager.get_async_response')
    def test_non_s3_responses_non_s3(self, mock_response):
        mocked = {'response': 'some_successful_response'}
        mock_response.return_value = mocked

        test_ids = ['mock_id-123', 'mock_id-456']
        response_datas = []
        num_responses_collected = 0

        response_datas, response_ids, num_responses_collected = try_getting_responses(response_ids=test_ids,
                                                                                      response_datas=response_datas,
                                                                                      num_responses_collected=num_responses_collected)

        test_ids.append('mock_id-789')
        mocked_failure = {'status': 'failed', 'response': None}
        mock_response.return_value = mocked_failure
        response_datas, response_ids, num_responses_collected = try_getting_responses(response_ids=test_ids,
                                                                                      response_datas=response_datas,
                                                                                      num_responses_collected=num_responses_collected)

        assert num_responses_collected == 3
        assert response_ids == []
        assert response_datas == ['some_successful_response', 'some_successful_response', mocked_failure]
        assert mock_response.call_count == 3
