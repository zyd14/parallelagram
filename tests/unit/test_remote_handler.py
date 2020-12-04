from time import sleep
from unittest.mock import patch

from parallelagram.exceptions import TaskException
from parallelagram.remote_handler import remote_handler, put_response_in_s3, remotely_run


class TestRemoteHandler:

    @patch('parallelagram.remote_handler.remotely_run')
    @patch('parallelagram.remote_handler.handle_exception')
    def test_no_timeout_error(self, mock_exc_handler, mock_remotely_run, monkeypatch):
        def mock_test_function(*args, **kwargs):
            sleep(1)
            return 'winner winner chicken dinner'

        def remotely_run_mock(func, *args, **kwargs):
            return func(*args, **kwargs)

        mock_exc_handler.return_value = None
        mock_remotely_run.return_value = remotely_run_mock(mock_test_function)

        test_mock_function = remote_handler(mock_test_function)

        mock_event = {'response_id': 'abc123',
                      'LOGIN_INFO': 'TEST'}

        class MockContext:

            def get_remaining_time_in_millis(self):
                return 20000

        result = test_mock_function(mock_event, MockContext())
        assert result == 'winner winner chicken dinner'

    @patch('parallelagram.remote_handler.handle_exception')
    def test_timeout_error(self, mock_exc_handler, monkeypatch):
        import asyncio

        def mock_test_function(*args, **kwargs):
            async def mock_sleep():
                await asyncio.sleep(5)

            asyncio.get_event_loop().run_until_complete(asyncio.gather(mock_sleep()))
            return 'winner winner chicken dinner'

        monkeypatch.setattr('parallelagram.remote_handler.remotely_run', mock_test_function)

        mock_exc_handler.return_value = None

        test_mock_function = remote_handler(mock_test_function)

        mock_event = {'response_id': 'abc123'}

        class MockContext:

            def get_remaining_time_in_millis(self):
                return 10500

        result = test_mock_function(mock_event, MockContext())
        assert result is None
        assert mock_exc_handler.call_count == 1
        assert mock_exc_handler.call_args_list[0].args[2] == 'TaskTimeoutError'


class TestPutResponseInS3:

    @patch('parallelagram.remote_handler.s3_client')
    def test_normal_json_response(self, mock_client, monkeypatch):
        mock_client.put_object.return_value = None
        monkeypatch.setenv('S3_RESPONSE_BUCKET', 'mock_bucket')

        test_response = {'some': 'values',
                         'and': 123,
                         'more': ['stuff', 'here']}
        test_result = put_response_in_s3(test_response)
        assert test_result.get('s3_response')
        assert test_result.get('s3_bucket') == 'mock_bucket'
        assert len(test_result.get('s3_key')) > 10
        assert mock_client.put_object.call_count == 1

    @patch('parallelagram.remote_handler.s3_client')
    def test_non_jsonable(self, mock_client, monkeypatch):
        mock_client.put_object.return_value = None
        monkeypatch.setenv('S3_RESPONSE_BUCKET', 'mock_bucket')

        test_response = {'some': 'values',
                         'and': 123,
                         'more': {'stuff', 'here'}}
        test_result = put_response_in_s3(test_response)
        assert test_result.get('s3_response')
        assert test_result.get('s3_bucket') == 'mock_bucket'
        assert len(test_result.get('s3_key')) > 10
        assert mock_client.put_object.call_count == 1


def mock_function_to_execute(response_to_return):
    return response_to_return


class MockException(Exception):
    pass


def mock_function_with_exception(*args, **kwargs):
    raise MockException()


class TestRemotelyRun:

    @patch('parallelagram.remote_handler.dynamo_client')
    def test_not_initial_no_s3_no_outputs_no_exit_do_capture_response(self, mock_client, monkeypatch):
        mock_client.update_item.return_value = None
        mock_client.put_item.return_value = None

        response_to_return = {'test': 'response'}
        response = remotely_run(func_to_execute=mock_function_to_execute,
                                capture_response=True,
                                response_id='abc123',
                                kwargs={'response_to_return': response_to_return})

        assert response == response_to_return
        assert mock_client.update_item.call_count == 1
        assert mock_client.put_item.call_count == 1

    def test_function_raises_task_exception(self):

        try:
            remotely_run(func_to_execute=mock_function_with_exception,
                         capture_response=False,
                         response_id='abc123')
        except Exception as exc:
            assert isinstance(exc, TaskException)
