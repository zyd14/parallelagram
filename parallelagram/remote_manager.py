""" A module for executing lambda functions asynchronously and retrieving their outputs so that the caller can continue
    processing after the remote lambda invocations have completed.  This is useful for farming out short-lived tasks to
    lambda functions from an EC2, ECS, local machine, or even another lambda function. It can be used to call any
    python function or set of functions deployed on a particular lambda from another remote process.

    Examples of usage can be found in src.example, with some test functions found in src.func_to_test.

    Basic usage has two different patterns, depending on whether your application is deployed via zappa or has been
    manually deployed with a manually-created handler. This difference in usage patterns is due to the fact that zappa
    dynamically creates a lambda handler on deployment, which requires the user to use some particular constructs and
    have zappa installed on their lambda function in order for the message passing and DynamoDB item creation to work
    correctly. Non-zappa deployed functions can make use a of a decorator provided in src.zappa_async_fork which can be
    used to wrap their handler function and will perform the DynamoDB logging that zappa usually would perform - in this
    case zappa is not required to be installed on the lambda to be invoked.

    Non-zappa deployed remote lambda usage and execution explanation:

    1) The user creates a lambda function to be invoked. From the zappa_async_fork module the user will import the
        @remote_handler decorator, which will be used to wrap the lambda_handler function required by AWS. This
        decorator is what will perform the DynamoDB logging which is needed in order to retrieve the result of the
        lambda invocation.
    2) From this remote_manager module the caller will import the Lambdable class, and the manage function.  The caller
        creates a number of Lambdable tasks and appends them to a list, each of which defines the module path of the
        function to be executed, the arguments to execute it with, and some arguments designating what lambda to run and
        how to run it.  This list is then used to invoke the manage() function.

        For non-zappa deployed lambdas, the remotely-invoked code does not need to live on the caller.
        **this is all the user needs to do from the caller side - an example can be found in the example.py file
        provided in this repo**

    Zappa-deployed remote lambda usage:

        If the user doesn't care about some of the built-in error handling provided by the @remote_wrapper decorator
        in zappa_async_fork.py, then the user can simply deploy their lambda function using the Zappa framework as
        usual, then follow step 2) as described above to execute the function(s) desired on the remote lambda. If the
        user does want to make use of the build-in error handling, they will need to make the following small
        adjustments:

    1) In the lambda function to be called, the user will need to import the @remote_runner decorator from this module
        (remote_manager.py) and will need to wrap any top-level functions they will want to execute (top-level meaning
        if the function the user wishes to execute calls other functions, only call the function at the top of the
        execution stack.  Note - decorated recursive functions may have odd and erroneous behavior). The user will then
        deploy their lambda function with the Zappa framework as usual.
    2) As described in step 2) for non-zappa functions, the caller will create Lambdable tasks which can be added to a
        list and used to invoke manage()

    Seeing as this pattern is essentially distributing parts of an application across different computing instances it
    is a bit confusing as to what goes where.  Here is an attempt to clarify this:
    There should be a codebase which is going to be executed on a longer-running instance, such as an EC2 or Docker
    container executed via AWS ECS or Batch, or even your local laptop / workstation. We'll call this your Manager
    instance. There should also exist another codebase with any smaller, short-lived functions which you would like to
    farm out tasks to from your Manager instance.  This codebase represents the code which will be executed as a
    remotely-invoked Lambda function (remotely invoked by the Manager instance).
    The Manager instance is where the application will start and end and will at some point be responsible for invoking
    Lambda functions to carry out tasks on its behalf. In order to do this the Manager instance will need to have the
    zappa_async_fork and remote_manager modules loaded on it.  The Manager instance will use the Lambdable class and
    remote_manager.manage() in order to invoke any Lambda functions it wants to farm out tasks to.  The
    remote_manager.manage() function is what will perform the work of invoking the Lambda functions defined by the
    Lambdables you created, and will then poll a DynamoDB table in a sleep() loop for items that represent the outputs
    of the Lambda workers. Once it has gathered all the outputs or has timed out, it will return them to your
    application, which can then move on and do more things like aggregate those results and perform further
    transformations.
    The Lambda functions that are being called will need to be deployed separately to AWS Lambda, and must have the
    zappa_async_fork module in their code.  To facilitate the creation and updating of the DynamoDB items that represent
    the results and status of a particular Lambda worker there is a @remote_handler decorator which should be imported
    from zappa_async_fork and used to wrap the handler of the Lambda function. When the handler is invoked, the
    @remote_handler function will be called and will intercept the event being passed to the Lambda handler. It will
    unpack a message dictionary that the Manager sent when remotely invoking the function, which contains an ID to store
    a DynamoDB item under, a path to a function on the Lambda to be executed, and any arguments to pass to the function.
    It will then 1) create a DynamoDB item indicating that the Lambda execution has started, 2) load the function on the
    Lambda to be executed 3) run the function being executed and finally 4) gather the response or any errors and write
    them to the DynamoDB item indicating the execution has completed.
    The whole time the Lambda worker(s) are executing, the Manager code is polling the DynamoDB items waiting for the
    Lambda workers to update them with their responses.  When the Manager code sees a response in the DynamoDB item
    for a particular Lambda worker, it removes that item from the list of items it is checking for responses from.
    Eventually it will have all the responses, and will return a list of responses to the caller of
    remote_manager.manage().

    Example:
        non-Zappa deployed Lambda:

        Lambda code (lambda_function.py), deployed to a function called lambda-worker:

            import pandas
            from zappa_async_fork import remote_handler

            @remote_handler
            def lambda_handler(event, context):
                data = event.get('data')
                return analyzer(data)

            def analyzer(data):
                df = pandas.DataFrame(data)
                # do some stuff
                return {'Results': df.to_dict()}

        Manager Code (executor.py):
            from remote_manager import Lambdable, manage
            task_list = []
            task_list.append(Lambdable(func_path='lambda_function.analyze',
                             remote_aws_lambda_func_name='kitchen_sink_analyze',
                             args = [{'r1': ['AACCTTGG', 'TTCCGG'], 'r2': ['CCTTGGAA', 'TTGGAACC']}],
                             kwargs = {})
                             )
            results = manage(task_list)

    Issue: as currently implemented, a Zappa-deployed lambda function with some of its functionality implemented
    using the @remote_runner decorator and called without the functionality provided by remote_manager.manage()
     will write items to DynamoDB unnecessarily, and will fail if the Dynamo functionality fails. It should still
     return the expected response to the caller if Dynamo logging works.

    Future implementation ideas:
        Provide ability to transparently package args and kwargs from Lambdables into S3, and also transparently
        retrieve and unpackage them from S3 in the remote lambda invocation before executing it.
"""

import json
import logging
import os
from functools import wraps, update_wrapper
from time import sleep
from typing import Callable, List, Tuple, Union
import uuid

import boto3

from parallelagram.utils import LOGGER
from parallelagram.launchables import TaskMap, Lambdable
from launchables import run
from utils import prep_s3_object, get_async_response

s3_client = boto3.client("s3", region_name="us-west-2")
ecs_client = boto3.client("ecs", region_name="us-west-2")


REQUEST_S3_BUCKET = os.getenv("REQUEST_S3_BUCKET", "sg-phil-testing")


def launch_remote_tasks(tasks: Union[TaskMap, List[Lambdable]]) -> List[str]:
    import launchables

    response_ids = []
    if isinstance(tasks, list):
        for lambdable in tasks:
            if lambdable.capture_response:
                # append to list of responses to check for in DynamoDB later
                lambdable.run_task()
                response_ids.append(lambdable.response_id)
            else:
                # didn't care about getting a response from this lambdable, don't add it to the list of responses to
                # check in on
                lambdable.run_task()
    elif isinstance(tasks, launchables.TaskMap):
        # Execute tasks in task map - TaskMap objects are supported to enable use with zappa-deployed functions
        for t, args in tasks:
            # When task is executed the @task wrapper provided by zappa returns an object which provides a DynamoDB
            # item ID under which the result of the remote lambda task will be stored.  This can be used with the
            # get_async_response function (also brought to you by zappa) in order to retrieve the response from DynamoDB
            response_ids.append(t(*args[0], **args[1]).response_id)
    else:
        error_msg = f"Unexpected task type {type(tasks)}"
        LOGGER.error(error_msg)
        raise TypeError(error_msg)

    return response_ids


def manage(task_map: Union[TaskMap, List[Lambdable]]) -> Union[List[str], None]:
    """Main method for executing tasks in a TaskMap as remote lambda functions, collecting their responses and
    returning them to the caller.

    Args:
        task_map: a TaskMap object holding tasks to be executed asynchronously

    Returns:
        list of response_ids identifying DynamoDB items to which remote lambda invocations will write the results of
        their execution.
    """

    if len(task_map) < 1:
        LOGGER.info("No tasks added")
        return

    response_ids = launch_remote_tasks(task_map)

    if not response_ids:
        LOGGER.info(
            "No responses collected as capture_response=False for all Lambdables. Either you don't care"
            "about responses from your workers or some error has occurred."
        )
        return

    total_wait = 0

    # Initial wait period gives the lambdas a period of time to get going before looking for their results
    initial_wait = int(os.getenv("INITIAL_WAIT_SECONDS", 5))
    sleep(initial_wait)
    total_wait += initial_wait

    max_total_wait = int(os.getenv("MAX_TOTAL_WAIT", 900))
    if max_total_wait > 900:
        LOGGER.info(
            "MAX WAIT set to more than 15 minutes (900 seconds) - remote lambda workers can only execute for a "
            "maximum of 15 minutes, so it is likely that they will start timing out after that time period. If "
            "expecting remote workers to execute for longer than 15 minutes, consider using a Fargate or Batch "
            "solution instead"
        )
    num_tasks = len(response_ids)

    if response_ids:
        response_datas = profit(response_ids, total_wait, max_total_wait, num_tasks)
        if len(response_datas) != num_tasks:
            LOGGER.warning(
                "Not all responses collected from tasks, return data will likely be incomplete"
            )

        # Error handling is pretty basic right now - zappa-executed async lambda tasks seem to consistently return
        # "N/A" as the 'response' key, so we can at least look for that to try to detect errors.
        # It's possible that "N/A" responses also indicate that the lambda task just didn't return anything - needs
        # more testing.  Catching all exceptions in remote lambda and returning something like "ERROR" might be a
        # more reliable way to detect errors.

        # UPDATE - the remote_runner wrapper provided in zappa_async_fork module provides some slightly better error
        # handling by returning tracebacks from remotely-invoked lambda to the DynamoDB table where responses are
        # stored, enabling us to actually get the traceback when get_async_response is called
        errors = check_for_errors(response_datas)
        if errors:
            LOGGER.error("Some errors were detected")
        return response_datas
    else:
        LOGGER.warning("No tasks created")


def run_lambdable_task(lambdable: Lambdable):
    """Invoke a remote lambda function specified by the Lambdable object, returning the response_id attribute of a
    DynamoDB item to which the remote lambda function will write its return value to.
    """

    if not isinstance(lambdable, Lambdable):
        raise Exception("Can't execute non-Lambdable tasks in lists yet")

    # Invoke the remote Lambda function with the arguments provided by the previously defined Lambdable
    lambdable.run_task()
    # TODO: check response for error codes


def check_for_errors(response_datas: List[dict]):
    errors = []
    for r in response_datas:
        if (
            r.get("response") == "N/A"
            or r.get("status") == "failed"
            or "UnhandledException" in r.get("response", "")
        ):
            errors.append(r)
    return errors


def profit(
    response_ids: List[str], total_wait: int, max_total_wait: int, num_tasks: int
) -> list:
    """Attempt to collect responses from tasks using response IDs returned when launching async lambda functions.

    Args:
        response_ids: List of response IDs corresponding to an item in DynamoDB where the response from a remote task
            will be stored
        total_wait: The amount of time that the manager has already waited for tasks to be finished
        max_total_wait: The maximum time the manager will wait for all responses to be collected before timing out and
            returning what it has
        num_tasks: Number of tasks being executed

    Returns:
        list of values returned by remote lambda invocations, usually represented as dictionaries
    """
    loop_wait = int(os.getenv("LOOP_WAIT_SECONDS", 15))

    response_datas = []
    num_responses_collected = 0

    # While there are still response_ids to collect and time hasn't maxed out, keep trying to get response data from
    # DynamoDB
    while len(response_ids) > 0 and total_wait < max_total_wait:
        LOGGER.info(f"Response IDs: {response_ids}")
        response_datas, response_ids, num_responses_collected = try_getting_responses(
            response_ids=response_ids,
            response_datas=response_datas,
            num_responses_collected=num_responses_collected,
        )

        # Not all responses collected yet, sleep for a user-specified amount of time before trying again
        if num_responses_collected != num_tasks:
            LOGGER.info("Didn't get all responses, going to sleep for a bit")
            sleep(loop_wait)
            total_wait += loop_wait
        else:
            # All responses gathered, return them to caller
            return response_datas

    if num_responses_collected == num_tasks:
        LOGGER.info("got all responses!")
        return response_datas
    elif total_wait >= max_total_wait:
        LOGGER.warning(
            "Timed out, returning what responses were collected but data is likely to be incomplete"
        )
        return response_datas


def try_getting_responses(
    response_ids: List[str], response_datas: List[dict], num_responses_collected: int
) -> Tuple[List[dict], List[str], int]:
    """Iterate over task response_ids, using each ID to look up an item in DynamoDB which will store the result of an
    individual lambda task when it has completed. If a response is found for a task, remove its ID from the list so
    it is not checked for again.
    """
    response_ids_collected = []
    for r in response_ids:
        response = get_async_response(r)
        if response is not None:
            # if lambda is still running then just log a message and don't do anything with that ID
            if (
                response.get("status") == "in progress"
                and response.get("response") == "N/A"
            ):
                LOGGER.info(f"lambda {r} still going, check back later")
            elif "fail" in response.get("status", ""):
                response_datas.append(response)
                response_ids_collected.append(r)
                num_responses_collected += 1
            else:
                if "s3_response" in response.get("response", {}) and response.get(
                    "response"
                ).get("s3_response"):
                    # Get response from S3
                    response_datas.append(get_s3_response(response.get("response")))
                    response_ids_collected.append(r)
                    num_responses_collected += 1
                else:
                    # Response was retrieved from S3, add it to responses that have been collected
                    response_datas.append(response.get("response"))
                    response_ids_collected.append(r)
                    num_responses_collected += 1

    # Remove response_ids that have been collected from responses we're still trying to get
    remove_collected_ids(response_ids_collected, response_ids)

    return response_datas, response_ids, num_responses_collected


def remove_collected_ids(response_ids_collected: List[str], response_ids: List[str]):
    """Remove any response IDs that have been collected already from the list of response IDs that are being looked
    for
    """
    for r in response_ids_collected:
        try:
            response_ids.remove(r)
        except ValueError:
            pass


def get_s3_response(response: dict) -> dict:
    """ Retrieve response from worker Lambda which stored its response in S3"""
    s3_bucket = response.get("s3_bucket")
    s3_key = response.get("s3_key")
    LOGGER.info(f"Retrieving data from s3://{s3_bucket}/{s3_key}")
    s3 = boto3.client("s3", region_name="us-west-2")
    return json.loads(
        s3.get_object(Bucket=s3_bucket, Key=s3_key)["Body"].read().decode("utf-8")
    )
