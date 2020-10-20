"""
Zappa Asynchronous Tasks

Note - this block comment only applies to the @task decorator as implemented in zappa and used in zappa packages, and
is somewhat irrelevant to methods added to this fork such as @remotely_run, although the general concept of passing a
lambda function a message defining a Dynamo item ID to create and store function status and outputs in, as well as the
path of the actual function to execute, are mostly the same.

Example:
```
from zappa.asynchronous import task

@task(service='sns')
def my_async_func(*args, **kwargs):
    dosomething()
```

The following restrictions apply:
* function must have a clean import path -- i.e. no closures, lambdas, or methods.
* args and kwargs must be JSON-serializable.
* The JSON-serialized form must be within the size limits for Lambda (128K) or SNS (256K) events.

Discussion of this comes from:
    https://github.com/Miserlou/Zappa/issues/61
    https://github.com/Miserlou/Zappa/issues/603
    https://github.com/Miserlou/Zappa/pull/694
    https://github.com/Miserlou/Zappa/pull/732
    https://github.com/Miserlou/Zappa/issues/840

## Full lifetime of an asynchronous dispatch:

1. In a file called `foo.py`, there is the following code:

```
   from zappa.asynchronous import task

   @task
   def my_async_func(*args, **kwargs):
       return sum(args)
```

2. The decorator desugars to:
   `my_async_func = task(my_async_func)`

3. Somewhere else, the code runs:
   `res = my_async_func(1,2)`
   really calls task's `_run_async(1,2)`
      with `func` equal to the original `my_async_func`
   If we are running in Lambda, this runs:
      LambdaAsyncResponse().send('foo.my_async_func', (1,2), {})
   and returns the LambdaAsyncResponse instance to the local
   context.  That local context, can, e.g. test for `res.sent`
   to confirm it was dispatched correctly.

4. LambdaAsyncResponse.send invoked the currently running
   AWS Lambda instance with the json message:

```
   { "command": "zappa.asynchronous.route_lambda_task",
     "task_path": "foo.my_async_func",
     "args": [1,2],
     "kwargs": {}
   }
```

5. The new lambda instance is invoked with the message above,
   and Zappa runs its usual bootstrapping context, and inside
   zappa.handler, the existence of the 'command' key in the message
   dispatches the full message to zappa.asynchronous.route_lambda_task, which
   in turn calls `run_message(message)`

6. `run_message` loads the task_path value to load the `func` from `foo.py`.
   We should note that my_async_func is wrapped by @task in this new
   context, as well.  However, @task also decorated `my_async_func.sync()`
   to run the original function synchronously.

   `run_message` duck-types the method and finds the `.sync` attribute
   and runs that instead -- thus we do not infinitely dispatch.

   If `my_async_func` had code to dispatch other functions inside its
   synchronous portions (or even call itself recursively), those *would*
   be dispatched asynchronously, unless, of course, they were called
   by: `my_async_func.sync(1,2)` in which case it would run synchronously
   and in the current lambda function.

"""
from time import time

from functools import update_wrapper, wraps
import importlib
import inspect
import logging
import json
import os
import uuid
from typing import Union

import boto3


ASYNC_RESPONSE_TABLE = 'phils_done_tasks'
dynamo_client = boto3.client('dynamodb', region_name='us-west-2')
# Declare these here so they're kept warm.
aws_session = boto3.Session()
LAMBDA_CLIENT = aws_session.client('lambda', region_name='us-west-2')

s3_client = boto3.client('s3', region_name='us-west-2')


def create_logger() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel('INFO')
    sh = logging.StreamHandler()
    sh.setLevel('INFO')
    logger.addHandler(sh)
    return logger


LOGGER = create_logger()

##
# Response and Exception classes
##

LAMBDA_ASYNC_PAYLOAD_LIMIT = 256000


class AsyncException(Exception):
    """ Simple exception class for async tasks. """
    pass


class LambdaAsyncResponse:
    """
    Base Response Dispatcher class
    Can be used directly or subclassed if the method to send the message is changed.
    """
    def __init__(self,
                 lambda_function_name: str = '',
                 aws_region: str = '',
                 capture_response: bool = False,
                 response_id: str = '',
                 **kwargs):
        """ """
        if kwargs.get('boto_session'):
            self.client = kwargs.get('boto_session').client('lambda')
        else:  # pragma: no cover
            self.client = LAMBDA_CLIENT

        self.lambda_function_name = lambda_function_name
        self.aws_region = aws_region
        if capture_response:
            if ASYNC_RESPONSE_TABLE is None:
                print(
                    "Warning! Attempted to capture a response without "
                    "async_response_table configured in settings (you won't "
                    "capture async responses)."
                )
                capture_response = False
                self.response_id = "MISCONFIGURED"

            else:
                if not response_id:
                    self.response_id = str(uuid.uuid4())
                else:
                    self.response_id = response_id
        else:
            self.response_id = None

        self.capture_response = capture_response

    def send(self,
             task_path: str,
             args: Union[tuple, list],
             kwargs: dict,
             get_request_from_s3: bool,
             request_s3_bucket: str,
             request_s3_key: str,
             response_to_s3: bool = False
             ):
        """
        Create the message object and pass it to the actual sender.
        request_s3_bucket: str = '',
                 request_s3_key: str = '',
                 response_to_s3: str = '',
        """
        message = {
                'task_path': task_path,
                'capture_response': self.capture_response,
                'response_id': self.response_id,
                'args': args,
                'kwargs': kwargs,
                'get_request_from_s3': get_request_from_s3,
                'request_s3_bucket': request_s3_bucket,
                'request_s3_key': request_s3_key,
                'response_to_s3': response_to_s3
            }
        self._send(message)
        return self

    def _send(self, message):
        """
        Given a message, directly invoke the lambda function for this task.
        """
        message['command'] = 'zappa.asynchronous.route_lambda_task'
        payload = json.dumps(message).encode('utf-8')
        if len(payload) > LAMBDA_ASYNC_PAYLOAD_LIMIT:  # pragma: no cover
            raise AsyncException("Payload too large for async Lambda call")
        self.response = self.client.invoke(
                                    FunctionName=self.lambda_function_name,
                                    InvocationType='Event',  # makes the call async
                                    Payload=payload
                                )
        self.sent = (self.response.get('StatusCode', 0) == 202)


ASYNC_CLASSES = {
    'lambda': LambdaAsyncResponse,
}

##
# Execution interfaces and classes
##


def run(func=None,
        args: list = None,
        kwargs: dict = None,
        capture_response: bool = False,
        remote_aws_lambda_function_name: str = None,
        remote_aws_region: str = None,
        task_path: str = '',
        response_id: str = '',
        get_request_from_s3: bool = False,
        request_s3_bucket: str = '',
        request_s3_key: str = '',
        response_to_s3: bool = False,
        **task_kwargs):
    """
    Instead of decorating a function with @task, you can just run it directly.
    If you were going to do func(*args, **kwargs), then you will call this:
request_s3_bucket: str = '',
                 request_s3_key: str = '',
                 response_to_s3: str = '',
    import zappa.asynchronous.run
    zappa.asynchronous.run(func, args, kwargs)

    and other arguments are similar to @task
    """
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    lambda_function_name = remote_aws_lambda_function_name or os.environ.get('AWS_LAMBDA_FUNCTION_NAME')
    aws_region = remote_aws_region or os.environ.get('AWS_REGION')

    if not task_path:
        task_path = get_func_task_path(func)

    return LambdaAsyncResponse(lambda_function_name=lambda_function_name,
                               aws_region=aws_region,
                               capture_response=capture_response,
                               response_id=response_id,
                               **task_kwargs).send(task_path=task_path,
                                                   args=args,
                                                   kwargs=kwargs,
                                                   get_request_from_s3=get_request_from_s3,
                                                   request_s3_bucket=request_s3_bucket,
                                                   request_s3_key=request_s3_key,
                                                   response_to_s3=response_to_s3)


# Handy:
# http://stackoverflow.com/questions/10294014/python-decorator-best-practice-using-a-class-vs-a-function
# However, this needs to pass inspect.getargspec() in handler.py which does not take classes
# Wrapper written to take optional arguments
# http://chase-seibert.github.io/blog/2013/12/17/python-decorator-optional-parameter.html
def task(*args, **kwargs):
    """Async task decorator so that running

    Args:
        func (function): the function to be wrapped
            Further requirements:
            func must be an independent top-level function.
                 i.e. not a class method or an anonymous function
        service (str): either 'lambda' or 'sns'
        remote_aws_lambda_function_name (str): the name of a remote lambda function to call with this task
        remote_aws_region (str): the name of a remote region to make lambda/sns calls against

    Returns:
        A replacement function that dispatches func() to
        run asynchronously through the service in question
    """

    func = None
    if len(args) == 1 and callable(args[0]):
        func = args[0]

    if not kwargs:  # Default Values
        service = 'lambda'
        lambda_function_name_arg = None
        aws_region_arg = None
        task_path = None
    else:  # Arguments were passed
        service = kwargs.get('service', 'lambda')
        lambda_function_name_arg = kwargs.get('remote_aws_lambda_function_name')
        aws_region_arg = kwargs.get('remote_aws_region')
        task_path = kwargs.get('task_path')

    capture_response = kwargs.get('capture_response', False)

    def func_wrapper(func):

        @wraps(func)
        def _run_async(*args, **kwargs):
            """
            This is the wrapping async function that replaces the function
            that is decorated with @task.
            Args:
                These are just passed through to @task's func

            Assuming a valid service is passed to task() and it is run
            inside a Lambda process (i.e. AWS_LAMBDA_FUNCTION_NAME exists),
            it dispatches the function to be run through the service variable.
            Otherwise, it runs the task synchronously.

            Returns:
                In async mode, the object returned includes state of the dispatch.
                For instance

                When outside of Lambda, the func passed to @task is run and we
                return the actual value.
            """
            lambda_function_name = lambda_function_name_arg or os.environ.get('AWS_LAMBDA_FUNCTION_NAME')
            aws_region = aws_region_arg or os.environ.get('AWS_REGION')

            send_result = LambdaAsyncResponse(lambda_function_name=lambda_function_name,
                                              aws_region=aws_region,
                                              capture_response=capture_response).send(task_path=task_path,
                                                                                      args=args,
                                                                                      kwargs=kwargs,
                                                                                      get_request_from_s3=False,
                                                                                      request_s3_bucket='',
                                                                                      request_s3_key='')
            return send_result

        update_wrapper(_run_async, func)

        _run_async.service = service
        _run_async.sync = func

        return _run_async

    return func_wrapper(func) if func else func_wrapper


##
# Utility Functions
##

def import_and_get_task(task_path):
    """
    Given a modular path to a function, import that module
    and return the function.
    """
    module, function = task_path.rsplit('.', 1)
    app_module = importlib.import_module(module)
    app_function = getattr(app_module, function)
    return app_function


def get_func_task_path(func):
    """
    Format the modular task path for a function via inspection.
    """
    module_path = inspect.getmodule(func).__name__
    task_path = '{module_path}.{func_name}'.format(
                                        module_path=module_path,
                                        func_name=func.__name__
                                    )
    return task_path


def get_async_response(response_id):
    """
    Get the response from the async table
    """
    response = dynamo_client.get_item(
        TableName=ASYNC_RESPONSE_TABLE,
        Key={'id': {'S': str(response_id)}}
    )
    if 'Item' not in response:
        return None

    return {
        'status': response['Item']['async_status']['S'],
        'response': json.loads(response['Item']['async_response']['S']),
    }


def remotely_run(task_path: str = '',
                 capture_response: bool = True,
                 response_id: str = '',
                 get_request_from_s3: bool = False,
                 request_s3_bucket: str = '',
                 request_s3_key: str = '',
                 response_to_s3: str = '',
                 args: list = None,
                 kwargs: dict = None):
    """
    Runs a function dynamically imported from a task_path provided as a key in the message parameter provided to
    remotely_run.  If the capture_response key of the message is also True, create an item in a DynamoDB table which
    will be used to track the status of the function being executed, as well as store the function's output for later
    retrieval by the remote invoker.
    """
    if get_request_from_s3:
        # Retrieve request from S3 - should have been written by remote_manager.manage() in caller code
        request = json.loads(s3_client.get_object(Bucket=request_s3_bucket,
                                                  Key=request_s3_key)['Body'].read().decode('utf-8'))
        args = request.get('args', [])
        kwargs = request.get('kwargs', {})
    if capture_response:
        # Create item in dynamo table showing task is in progress
        dynamo_client.put_item(
            TableName=ASYNC_RESPONSE_TABLE,
            Item={
                'id': {'S': str(response_id)},
                'ttl': {'N': str(int(time()+900))},
                'async_status': {'S': 'in progress'},
                'async_response': {'S': str(json.dumps('N/A'))},
            }
        )
    func = import_and_get_task(task_path)
    # Run function
    try:
        response = func(
            *args,
            **kwargs
        )
        status = 'completed'
        if response_to_s3:
            response_key = str(uuid.uuid4())
            response_bucket = os.getenv('S3_RESPONSE_BUCKET', 'sg-phil-testing')
            s3_client.put_object(Body=bytes(json.dumps(response).encode('UTF-8')),
                                 Bucket=response_bucket,
                                 Key=response_key)
            LOGGER.info(f'Wrote response to S3 at s3://{response_bucket}/{response_key}')
            response = {'s3_response': True, 's3_bucket': response_bucket, 's3_key': response_key}
    except Exception as exc:
        status = 'failed'
        import traceback
        logger = logging.getLogger()
        tb = traceback.format_exc()
        logger.error(f'Unhandled exception occurred: {exc}')
        logger.error(f'{tb}')
        response = {'UnhandledException': tb}

    if capture_response:
        # Update dynamo item with completed or failed status and response from function or traceback if
        # exception occurred
        dynamo_client.update_item(
            TableName=ASYNC_RESPONSE_TABLE,
            Key={'id': {'S': str(response_id)}},
            UpdateExpression="SET async_response = :r, async_status = :s",
            ExpressionAttributeValues={
                ':r': {'S': str(json.dumps(response))},
                ':s': {'S': status},
            },
        )

    return response


def remote_handler(func):
    """ This decorator can be used to decorate a lambda function handler, so that when the decorated handler is invoked
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
        if event.get('response_id'):
            return remotely_run(task_path=event.get('task_path'),
                                capture_response=event.get('capture_response'),
                                response_id=event.get('response_id'),
                                args=event.get('args'),
                                kwargs=event.get('kwargs'),
                                get_request_from_s3=event.get('get_request_from_s3'),
                                request_s3_bucket=event.get('request_s3_bucket'),
                                request_s3_key=event.get('request_s3_key'),
                                response_to_s3=event.get('response_to_s3')
                                )
        else:
            return func(event, context)
    return wrapped
