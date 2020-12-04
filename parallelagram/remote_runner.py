"""
Adapted from the Zappa serverless framework: https://github.com/Miserlou/Zappa

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
import inspect
import json
import os
import uuid
from typing import Dict, Optional, Union

from parallelagram.exceptions import AsyncException

from parallelagram.config import ASYNC_RESPONSE_TABLE, LAMBDA_ASYNC_PAYLOAD_LIMIT
from parallelagram.utils import dynamo_client
from utils import lambda_client


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
            self.client = lambda_client

        self.sent = False
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
        self.lambda_invoke_response = None
        self.capture_response = capture_response

    def send(self,
             task_path: str,
             args: Union[tuple, list],
             kwargs: dict,
             get_request_from_s3: bool = False,
             request_s3_bucket: str = None,
             request_s3_key: str = None,
             response_to_s3: bool = False,
             result_id: int = None,
             login_info: dict = None
             ):
        """
        Create the message object and pass it to the actual sender.
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
                'response_to_s3': response_to_s3,
                'result_id': result_id,
                'login_info': login_info
            }
        self._send(message)
        return self

    def _send(self, message: dict):
        """
        Given a message, directly invoke the lambda function for this task.
        """
        message['command'] = 'zappa.asynchronous.route_lambda_task'
        payload = json.dumps(message).encode('utf-8')
        if len(payload) > LAMBDA_ASYNC_PAYLOAD_LIMIT:  # pragma: no cover
            raise AsyncException("Payload too large for async Lambda call")
        self.lambda_invoke_response = self.client.invoke(
                                    FunctionName=self.lambda_function_name,
                                    InvocationType='Event',  # makes the call async
                                    Payload=payload
                                )
        # TODO: handle non-202 errors
        self.sent = (self.lambda_invoke_response.get('StatusCode', 0) == 202)


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
        result_id: int = None,
        login_info: dict = None,
        **task_kwargs) -> LambdaAsyncResponse:
    """ Run a function remotely on a separate Lambda instance by specifying either the actual function object (if it
        has the same module path in this calling code base as in the Lambda package being called) or a modular path to
        the function on the Lambda package being called (as a string). The arguments to call the function with can be
        specified by args and kwargs, while a number of metadata options can be included to support automatic Registrar
        updates and optional use of S3 to store requests and responses (for Lambdas being called that take >256kb
        request data, or that generate >400kb responses that need to be retrieved.
    """
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}
    if not remote_aws_region:
        remote_aws_region = os.getenv('AWS_REGION')

    if not task_path:
        task_path = get_func_task_path(func)

    return LambdaAsyncResponse(lambda_function_name=remote_aws_lambda_function_name,
                               aws_region=remote_aws_region,
                               capture_response=capture_response,
                               response_id=response_id,
                               **task_kwargs).send(task_path=task_path,
                                                   args=args,
                                                   kwargs=kwargs,
                                                   get_request_from_s3=get_request_from_s3,
                                                   request_s3_bucket=request_s3_bucket,
                                                   request_s3_key=request_s3_key,
                                                   response_to_s3=response_to_s3,
                                                   result_id=result_id,
                                                   login_info=login_info)


##
# Utility Functions
##


def get_async_response(response_id: str) -> Optional[Dict[str, Union[str, dict]]]:
    """
    Get a response from the DynamoDB table responsible for holding responses
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