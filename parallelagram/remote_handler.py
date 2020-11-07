import importlib
import inspect
import json
import os
import traceback
import uuid
from functools import wraps
from threading import Thread
from time import time, sleep
from typing import Union

from exceptions import TaskException, TaskTimeoutError
from utils import ASYNC_RESPONSE_TABLE, dynamo_client, LOGGER, s3_client


def import_and_get_task(task_path):
    """
    Given a modular path to a function, import that module
    and return the function.
    """
    module, function = task_path.rsplit(".", 1)
    app_module = importlib.import_module(module)
    app_function = getattr(app_module, function)
    return app_function


def get_func_task_path(func):
    """
    Format the modular task path for a function via inspection.
    """
    module_path = inspect.getmodule(func).__name__
    task_path = "{module_path}.{func_name}".format(
        module_path=module_path, func_name=func.__name__
    )
    return task_path


def remotely_run(
    task_path: str = "",
    capture_response: bool = True,
    response_id: str = "",
    get_request_from_s3: bool = False,
    request_s3_bucket: str = "",
    request_s3_key: str = "",
    response_to_s3: str = "",
    args: list = None,
    kwargs: dict = None,
):
    """
    Runs a function dynamically imported from a task_path provided as a key in the message parameter provided to
    remotely_run.  If the capture_response key of the message is also True, create an item in a DynamoDB table which
    will be used to track the status of the function being executed, as well as store the function's output for later
    retrieval by the remote invoker.
    """
    if get_request_from_s3:
        # Retrieve request from S3 - should have been written by remote_manager.manage() in caller code
        request = json.loads(
            s3_client.get_object(Bucket=request_s3_bucket, Key=request_s3_key)["Body"]
            .read()
            .decode("utf-8")
        )
        args = request.get("args", [])
        kwargs = request.get("kwargs", {})
    if capture_response:
        # Create item in dynamo table showing task is in progress
        dynamo_client.put_item(
            TableName=ASYNC_RESPONSE_TABLE,
            Item={
                "id": {"S": str(response_id)},
                "ttl": {"N": str(int(time() + int(os.getenv('RESPONSE_TTL', 900))))},
                "async_status": {"S": "in progress"},
                "async_response": {"S": str(json.dumps("N/A"))},
            },
        )
    func = import_and_get_task(task_path)
    # Run function
    try:
        response = func(*args, **kwargs)
    except Exception as exc:
        error_msg = f'Unhandled exception occurred while executing function {func.__name__}. Exception: {exc}'
        LOGGER.error(error_msg)
        tb = traceback.format_exc()
        LOGGER.error(f'{tb}')
        raise TaskException(task_exception=exc, tb=tb)

    status = "completed"
    if response_to_s3:
        response = put_response_in_s3(response)

    if capture_response:
        # Update dynamo item with completed or failed status and response from function or traceback if
        # exception occurred
        update_run_table_item(response_id=response_id,
                              response=response,
                              status=status)

    return response


def remote_handler(func):
    """This decorator can be used to decorate a lambda function handler, so that when the decorated handler is invoked
            asynchronously with a message key in the event it will use the remotely_run function to execute the local
            function defined by the 'task_path' key in the message dict, as well as store execution status and output in a
            DynamoDB item for later retrieval by the invoker.

            If a message key is not present in the event then the decorated function handler will be invoked with the raw
            event and context arguments as a handler usually would be.

            Because zappa dynamically generates its handler function on deployment, it is currently not possible to use this
            wrapper on zappa-deployed functions.  Users who deploy their lambdas via zappa should use the @task (original
            zappa wrapper) or @remote_runner function wrappers instead, which should both still play nice with the manager
            functionalities provided in remote_manager.

            Example for a non-Zappa deployed function:

    get_request_from_s3: bool = False,
                     request_s3_bucket: str = '',
                     request_s3_key: str = '',
                     response_to_s3: str = ''
    """

    @wraps(func)
    def wrapped(event: dict, context: dict = None):
        try:
            timing_thread = Thread(target=thread_timer, args=[context])
            timing_thread.start()
            if event.get("response_id"):
                return remotely_run(
                    task_path=event.get("task_path"),
                    capture_response=event.get("capture_response"),
                    response_id=event.get("response_id"),
                    args=event.get("args"),
                    kwargs=event.get("kwargs"),
                    get_request_from_s3=event.get("get_request_from_s3"),
                    request_s3_bucket=event.get("request_s3_bucket"),
                    request_s3_key=event.get("request_s3_key"),
                    response_to_s3=event.get("response_to_s3"),
                )
            else:
                return func(event, context)
        except TaskTimeoutError as te:
            # Timing thread noticed that invocation time was almost at timeout limit for this lambda - log failure
            error_msg = f'Lambda invocation with event {event} for function {func.__name__} timed out'
            LOGGER.error(error_msg)
            update_run_table_item(event.get('response_id'), 'TimeoutError', status='FAILED')
            raise te
        except TaskException as te:
            # An unhandled exception was caught when running the actual function user requested be executed
            error_msg = f'Unhandled exception occurred within lambda function code for function {func.__name__}. ' \
                        f'Exception: {te.task_exception}'
            LOGGER.error(error_msg)
            update_run_table_item(event.get('response_id'), 'TaskException', status='FAILED')
            raise te
        except Exception as exc:
            tb = traceback.format_exc()
            LOGGER.error(f'Unhandled exception occurred while executing {func.__name__}, '
                         f'most likely in wrapper handler code. Exception: {exc}')
            LOGGER.error(f'{tb}')
            response = {'UnhandledException': tb}
            update_run_table_item(event.get('response_id'), response, status='FAILED')
    return wrapped


def thread_timer(context):
    sleep((context.get_remaining_time_in_millis() - 3000)/1000)
    raise TaskTimeoutError


def update_run_table_item(response_id: str, response: Union[dict, str], status: str):
    # TODO: Add exception to item if status=FAILED
    if isinstance(response, dict):
        out_response = json.dumps(response)
    else:
        out_response = response
    dynamo_client.update_item(
        TableName=ASYNC_RESPONSE_TABLE,
        Key={'id': {'S': str(response_id)}},
        UpdateExpression="SET async_response = :r, async_status = :s",
        ExpressionAttributeValues={
            ':r': {'S': out_response},
            ':s': {'S': status},
        },
    )


def put_response_in_s3(response: dict) -> dict:
    response_key = str(uuid.uuid4())
    response_bucket = os.getenv("S3_RESPONSE_BUCKET", "sg-phil-testing")
    try:
        response_out = json.dumps({'remote_response': response})
    except json.JSONEncoder:
        response_out = str(response)
    s3_client.put_object(
        Body=bytes(response_out.encode("UTF-8")),
        Bucket=response_bucket,
        Key=response_key,
    )
    LOGGER.info(
        f"Wrote response to S3 at s3://{response_bucket}/{response_key}"
    )
    return {
        "s3_response": True,
        "s3_bucket": response_bucket,
        "s3_key": response_key,
    }