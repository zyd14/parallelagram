from unittest.mock import patch

from parallelagram.launchables import Lambdable


class TestLambdableRunTask:

    @patch('parallelagram.launchables.LambdaAsyncResponse.send')
    def test_no_request_to_s3(self, mock_send, mock_async_response):
        mock_send.return_value = mock_async_response
        test_lambdable = Lambdable(func_path='some.func.path',
                                   remote_aws_lambda_func_name='some-lambda-func',
                                   args=[],
                                   kwargs={})
        test_lambdable.run_task()
        assert len(test_lambdable.response_id) > 20 and isinstance(test_lambdable.response_id, str)
        assert mock_send.call_count == 1

    @patch('parallelagram.launchables.LambdaAsyncResponse.send')
    @patch('parallelagram.utils.s3_client.put_object')
    def test_with_request_to_s3(self, mock_put, mock_send, mock_async_response):
        mock_put.return_value = None
        mock_send.return_value = mock_async_response

        test_lambdable = Lambdable(func_path='some.func.path',
                                   remote_aws_lambda_func_name='some-lambda-func',
                                   args=[],
                                   kwargs={},
                                   request_to_s3=True)

        test_lambdable.run_task()
        assert test_lambdable.response_id
        assert mock_put.call_count == 1


class TestLambdableCheckForError:

    def test_na_error(self, mock_base_lambdable):
        mock_base_lambdable._response = "N/A"
        mock_base_lambdable.check_for_error()
        assert mock_base_lambdable.error

    def test_status_failed_error(self, mock_base_lambdable):
        mock_base_lambdable._response_status = 'FAILED'
        mock_base_lambdable.check_for_error()
        assert mock_base_lambdable.error

    def test_unhandled_exception_error(self, mock_base_lambdable):
        mock_base_lambdable._response = 'UnhandledException: Some stack trace'
        mock_base_lambdable.check_for_error()
        assert mock_base_lambdable.error

    def test_no_error(self, mock_base_lambdable):
        mock_base_lambdable._response = {'data': 123}
        mock_base_lambdable.check_for_error()
        assert not mock_base_lambdable.error


class TestLambdableTryGettingResponse:

    @patch('parallelagram.launchables.get_async_response')
    def test_normal_success_response_retrieved(self, mock_response, mock_base_lambdable):
        mocked = {'response': 'some_successful_response'}
        mock_response.return_value = mocked
        mock_base_lambdable.response_id = '123'

        mock_base_lambdable.try_getting_response()
        assert mock_base_lambdable.get_response() == 'some_successful_response'

    @patch('parallelagram.launchables.get_s3_response')
    @patch('parallelagram.launchables.get_async_response')
    def test_success_s3_response_retrieved(self, mock_response, mock_s3_response, mock_base_lambdable):
        mocked = {'response': 'some_successful_response', 'status': 'success'}
        mock_response.return_value = mocked
        mock_base_lambdable.response_id = '123'

        mock_s3_response.return_value = 'some_successful_response'

        mock_base_lambdable.try_getting_response()
        assert mock_base_lambdable.get_response() == 'some_successful_response'

    @patch('parallelagram.launchables.get_async_response')
    def test_in_progress_no_response_recorded(self, mock_response, mock_base_lambdable):
        mocked = {'response': 'N/A', 'status': 'in progress'}
        mock_response.return_value = mocked
        mock_base_lambdable.response_id = '123'

        mock_base_lambdable.try_getting_response()
        assert not mock_base_lambdable.get_response()
