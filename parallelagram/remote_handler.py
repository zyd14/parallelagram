import asyncio
import json
import os
import traceback
import uuid
from functools import wraps
from time import time
from typing import Union, Callable

from parallelagram.exceptions import TaskException, TaskTimeoutError
from parallelagram.utils import ASYNC_RESPONSE_TABLE, dynamo_client, LOGGER, s3_client, import_and_get_task


def remotely_run(
    func_to_execute: Callable,
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
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

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
        create_run_table_item(response_id)

    # Run function
    try:
        response = func_to_execute(*args, **kwargs)
    except Exception as exc:
        error_msg = f'Unhandled exception occurred while executing function {func_to_execute.__name__}. ' \
                    f'Exception: {exc}'
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


def create_run_table_item(response_id: str):
    """ Create item in dynamo table showing task is in progress"""
    dynamo_client.put_item(
        TableName=ASYNC_RESPONSE_TABLE,
        Item={
            "id": {"S": str(response_id)},
            "ttl": {"N": str(int(time() + int(os.getenv('RESPONSE_TTL', 900))))},
            "async_status": {"S": "in progress"},
            "async_response": {"S": str(json.dumps("N/A"))},
        },
    )


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
    def wrapped(event: dict, context):
        try:
            try:
                loop = asyncio.get_event_loop()
                result = [r for r in loop.run_until_complete(gather_results(context, event, func)) if
                          not isinstance(r, asyncio.exceptions.CancelledError)][0]
            except asyncio.exceptions.CancelledError:
                raise TaskTimeoutError()
            return result
        except TaskTimeoutError:
            # Timing thread noticed that invocation time was almost at timeout limit for this lambda - log failure
            error_msg = f'Lambda invocation with event {event} for function {func.__name__} timed out'
            handle_exception(error_msg, event.get('response_id'), 'TaskTimeoutError')
        except TaskException as task_exception:
            # An unhandled exception was caught when running the actual function user requested be executed
            error_msg = f'Unhandled exception occurred within lambda function code for function {func.__name__}. ' \
                        f'Exception: {task_exception.task_exception}'
            handle_exception(error_msg, event.get('response_id'), 'TaskException')
        except Exception as exc:
            error_msg = f'Unhandled exception occurred while executing {func.__name__}, '\
                         f'most likely in wrapper handler code. Exception: {exc}'
            handle_exception(error_msg, event.get('response_id'), 'UnhandledException')
    return wrapped


def handle_exception(error_msg: str, response_id: str, exc_name: Union[dict, str]):
    tb = traceback.format_exc()
    LOGGER.error(error_msg)
    LOGGER.error(f'{tb}')
    response = {exc_name: tb}
    update_run_table_item(response_id, response, status='FAILED')


async def gather_results(context, event: dict, func):
    return await asyncio.gather(*[thread_timer(context),
                                  handler_runner(event, func, context)], return_exceptions=True)


async def handler_runner(event: dict, func, context):

    if event.get('response_id') or event.get('initial_invocation'):

        if event.get('task_path'):
            # Retrieve function to execute from function module path
            func_to_execute = import_and_get_task(event.get('task_path'))
        else:
            # If task_path wasn't provided, then execute the wrapped handler
            func_to_execute = func

        result = remotely_run(func_to_execute=func_to_execute,
                              capture_response=event.get('capture_response'),
                              response_id=event.get('response_id'),
                              args=event.get('args'),
                              kwargs=event.get('kwargs'),
                              get_request_from_s3=event.get('get_request_from_s3'),
                              request_s3_bucket=event.get('request_s3_bucket'),
                              request_s3_key=event.get('request_s3_key'),
                              response_to_s3=event.get('response_to_s3'),
                              )
        # Cancel thread timer task so function cleanup can complete
        [t.cancel() for t in asyncio.all_tasks() if 'thread_timer' in str(t)]
        return result
    else:
        # Execute the wrapped handler with the raw event and context, without performing Registrar updates or
        # DynamoDB response / status tracking
        result = func(event, context)
        [t.cancel() for t in asyncio.all_tasks() if 'thread_timer' in str(t)]

        return result


async def thread_timer(context):
    await asyncio.sleep((context.get_remaining_time_in_millis() - 10000) / 1000)
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    raise TaskTimeoutError()


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
    except TypeError:
        response_out = str(response)
    s3_client.put_object(
        Body=bytes(response_out.encode('UTF-8')),
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