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
from typing import Any, Callable, Dict, List, Tuple, Union
import uuid

import boto3

from parallelagram.exceptions import EcsTaskConfigurationError, UnableToDetermineContainerName
from parallelagram.zappa_async_fork import get_async_response, task, get_func_task_path, run

s3_client = boto3.client('s3', region_name='us-west-2')
ecs_client = boto3.client('ecs', region_name='us-west-2')

def create_logger() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel('INFO')
    sh = logging.StreamHandler()
    sh.setLevel('INFO')
    logger.addHandler(sh)
    return logger


LOGGER = create_logger()
REQUEST_S3_BUCKET = os.getenv('REQUEST_S3_BUCKET', 'sg-phil-testing')


class Lambdable:

    def __init__(self,
                 func_path: str,
                 remote_aws_lambda_func_name: str,
                 args: Union[tuple, list] = None,
                 kwargs: dict = None,
                 capture_response: bool = True,
                 remote_aws_region: str = 'us-west-2',
                 request_to_s3: bool = False,
                 response_to_s3: bool = False):
        """
            Args:
                func_path: module path of function on remote lambda to be executed
                remote_aws_lambda_func_name: name of remote lambda function to be executed
                args: positional arguments to pass to function on remote lambda specified by 'func_path'
                kwargs: keyword arguments to pass to function on remote lambda specified by 'func_path'
                capture_response: indicates whether to store response from remote lambda in a DynamoDB table
                    for later retrieval.
                remote_aws_region: region to execute remote lambda function in - the lambda function being executed must
                    be deployed in this region.
                request_to_s3: whether to store the remote lambda request in S3 for retrieval by the remote lambda being
                    called.  This is done transparently by manage(), with the @remote_manager decorator performing
                    retrieval and unpacking of the request from S3 so that the target function being executed doesn't
                    need to be modified at all. Useful when requests exceed 256Kb, the AWS-imposed limit on asynchronous
                    lambda invocation payloads. Requires that capture_response = True.
                response_to_s3: whether to store the response from a remote lambda invocation in S3. This is performed
                    transparently by the @remote_manager decorator on the remote lambda being executed, storing whatever
                    is returned by the remote lambda in a S3 bucket and writing references to the object in the DynamoDB
                    response table. The manage() function will then retrieve the object from S3 and unpack it before
                    returning it to the caller. Useful when remote lambda responses exceed 400Kb, the AWS-imposed limit
                    on DynamoDB items. Requires that capture_response = True.
        """
        self.func_path = func_path
        if args is None:
            args = []
        self.args = args
        if kwargs is None:
            kwargs = {}
        self.kwargs = kwargs
        self.capture_response = capture_response

        if not remote_aws_lambda_func_name:
            error_msg = 'No remote_aws_lambda_func_name parameter provided'
            LOGGER.error(error_msg)
            raise Exception(error_msg)

        if not capture_response and response_to_s3:
            error_msg = 'User requested not to capture response but to store response in S3; response will from ' \
                        'for this Lambdable will be stored in S3 but may be difficult to retrieve from S3 due to it ' \
                        'being stored under a UUID key.'
            LOGGER.warning(error_msg)

        self.remote_aws_lambda_func_name = remote_aws_lambda_func_name
        self.remote_aws_region = remote_aws_region
        self.request_to_s3 = request_to_s3
        self.response_to_s3 = response_to_s3


class EcsTask:
    """ Class used to launch a single-container task from an existing Fargate task
        TODO: support multiple container tasks
    """
    def __init__(self,
                 cluster: str,
                 task_definition: str,
                 command: List[str] = None,
                 environment: Dict[str, str] = None,
                 task_cpu: int = None,
                 task_memory: int = None,
                 container_name: str = None,
                 container_cpu: int = None,
                 container_memory: int = None,
                 launch_type: str = 'FARGATE',
                 subnets: List[str] = None,
                 security_groups: List[str] = None):
        """
            Args:
                cluster: ECS cluster to execute task on
                task_definition: ECS task to run - must already have been registered with AWS
                command: Command-line string to pass to container, with each argument as a separate string value in a
                    list
                environment: Dictionary of environment keys and their values which can be used to override or supplement
                    environment variables for a given task
                task_cpu: Override for the total CPU allocated to a task
                task_memory: Override for the total memory allocated to a task
                container_name: Name of container to provide overrides to. If no container_name is provided and the
                    task is a single-container task then the container name will be looked up. If container_name is not
                    provided and multiple containers are found on the task then an exception will be thrown if any
                    container overrides are provided as it is impossible to determine which container to provide the
                    overrides to.
                container_cpu: Override for the CPU allocated to a specific container in a task
                container_memory: Override for the memory allocated to a specific container in a task
                launch_type: Defines whether task should be launched on existing ECS instances (EC2) or via Fargate (FARGATE)
                subnets: A list of subnets to launch tasks in. Required if the task definition networkMode = 'awsvpc'
                security_groups: A list of security groups to apply to the task. Required if the task definition
                    networkMode = 'awsvpc'
        """
        self.cluster = cluster
        self.task_definition = task_definition
        self.command = command
        self.environment = environment
        self.task_cpu = task_cpu
        self.task_memory = task_memory
        self.container_name = container_name
        self.container_cpu = container_cpu
        self.container_memory = container_memory
        self.launch_type = launch_type
        self.subnets = subnets
        self.security_groups = security_groups

        if self.container_memory or self.container_cpu and not self.container_name:
            self.container_name = self.get_container_name()

        if self.container_memory or self.container_cpu:
            self.container_overrides = True

        if self.task_memory or self.task_cpu:
            self.task_overrides = True

        self._aws_task_description: dict = ecs_client.describe_task_definition(taskDefinition=self.task_definition)['taskDefinition']

        if self._aws_task_description.get('networkMode') == 'awsvpc':
            self._awsvpc_task = True
        else:
            self._awsvpc_task = False

        self.validate()

    def validate(self):
        """ Check that values provided for task meet AWS-imposed requirements for running a task."""
        task_errors = []
        if self.task_cpu and self.task_cpu % 128 != 0:
            task_errors.append(f'task_cpu must be a multiple of 128')
        if self.container_cpu and self.container_cpu % 128 != 0:
            task_errors.append(f'container_cpu must be a multiple of 128')
        if self.launch_type == 'FARGATE':
            valid_memory_values = {256: [512, 1024, 2048],
                                   512: [1024, 2048, 3072, 4096],
                                   1024: [1024*i for i in range(2, 9)],
                                   2048: [1024*i for i in range(4, 17)],
                                   4096: [1024*i for i in range(8, 31)]}
            valid_cpu_values = list(valid_memory_values.keys())
            if self.task_cpu and self.task_cpu not in valid_cpu_values:
                task_errors.append(f'FARGATE task_cpu must be one of the following values: {valid_cpu_values}')
            else:
                if self.task_memory and self.task_cpu and self.task_memory not in valid_memory_values[self.task_cpu]:
                    task_errors.append(f'FARGATE task_memory for task_cpu value {self.task_cpu} is restricted by AWS to'
                                       f'the following values: {valid_memory_values[self.task_cpu]}')
            if self.container_cpu and self.container_cpu not in valid_cpu_values:
                task_errors.append(f'FARGATE container_cpu must be one of the following values: {valid_cpu_values}')

        if self.launch_type not in ['FARGATE', 'EC2']:
            task_errors.append(f'Task launch_type must be EC2 or FARGATE')

        network_mode = self._aws_task_description.get('networkMode')
        if network_mode == 'awsvpc' and (not self.subnets or not self.security_groups):
            task_errors.append(f'Tasks which have been defined with awsvpc networkMode must include at least one '
                               f'security group and at least one subnet.')

        if task_errors:
            for error_msg in task_errors:
                LOGGER.error(error_msg)
            raise EcsTaskConfigurationError(errors=task_errors)

    def get_container_name(self) -> str:
        """ Retrieve the container name from the described task definition"""

        containers: List[dict] = self._aws_task_description['containerDefinitions']
        if len(containers) > 1:
            error_msg = 'Multiple containers detected for task {self.task_definition}. Unable to determine container ' \
                        'name. container_name must be provided for tasks with container overrides that have more than '\
                        'one container'
            LOGGER.error(error_msg)
            raise UnableToDetermineContainerName(error_msg)
        else:
            container_name = containers[0].get('name')

        return container_name

    def generate_run_task_request(self) -> dict:
        """ Generate a request body to be used with a runTask() request."""
        run_task_request = {'cluster': self.cluster,
                            'launchType': self.launch_type,
                            'taskDefinition': self.task_definition
                            }

        task_overrides = {}
        # Compile task-wide overrides
        if self.task_overrides:
            if self.task_cpu:
                task_overrides.update({'cpu': self.task_cpu})
            if self.task_memory:
                task_overrides.update({'memory': self.task_memory})

        # Compile container-specific overrides
        if self.container_overrides:
            container_overrides = {'name': self.container_name}
            if self.container_cpu:
                container_overrides.update({'cpu': self.container_cpu})
            if self.container_memory:
                container_overrides.update({'memory': self.container_memory})
            if self.command:
                container_overrides.update({'command': self.command})
            if self.environment:
                container_overrides.update({'environment': [{'name': key, 'value': value} for key, value in self.environment.items()]})

            task_overrides.update({'containerOverrides': [container_overrides]})

        if task_overrides:
            run_task_request.update({'overrides': task_overrides})

        # Add awsvpc-task specific subnet / security group information
        if self._awsvpc_task:
            network_configuration = {'awsvpcConfiguration': {
                'subnets': self.subnets,
                'securityGroups': self.security_groups
            }}
            run_task_request.update({'networkConfiguration': network_configuration})

        return run_task_request


class TaskMap:
    """ Object to hold tasks to be executed on a remote lambda invocation"""

    def __init__(self):
        self._task_map = {}  # type: Dict[Callable, List[Tuple[List[Any], Dict[str, Any]]]]

    def __iter__(self) -> Tuple[Callable, Tuple[list, dict]]:
        for t in self._task_map:
            for arg_set in self._task_map[t]:
                yield t, arg_set

    def __len__(self):
        return len(self._task_map.keys())

    def add_task(self, remote_task: Union[Callable, str], args: List[Any] = None, kwargs: Dict[str, Any] = None):
        """

        Args:
            remote_task: function to be executed remotely
            args: list of positional arguments to be fed to task function
            kwargs: dictionary of keyword arguments to be fed to task function
        """
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        if remote_task not in self._task_map:
            self._task_map.update({remote_task: [(args, kwargs)]})
        else:
            self._task_map[remote_task].append((args, kwargs))


def launch_remote_tasks(tasks: Union[TaskMap, List[Lambdable]]) -> List[str]:
    response_ids = []
    if isinstance(tasks, list):
        for lambdable in tasks:
            if lambdable.capture_response:
                # append to list of responses to check for in DynamoDB later
                response_ids.append(run_lambdable_task(lambdable))
            else:
                # didn't care about getting a response from this lambdable, don't add it to the list of responses to
                # check in on
                run_lambdable_task(lambdable)
    elif isinstance(tasks, TaskMap):
        # Execute tasks in task map - TaskMap objects are supported to enable use with zappa-deployed functions
        for t, args in tasks:
            # When task is executed the @task wrapper provided by zappa returns an object which provides a DynamoDB
            # item ID under which the result of the remote lambda task will be stored.  This can be used with the
            # get_async_response function (also brought to you by zappa) in order to retrieve the response from DynamoDB
            response_ids.append(t(*args[0], **args[1]).response_id)
    else:
        error_msg = f'Unexpected task type {type(tasks)}'
        LOGGER.error(error_msg)
        raise TypeError(error_msg)

    return response_ids


def manage(task_map: Union[TaskMap, List[Lambdable]]) -> Union[List[str], None]:
    """ Main method for executing tasks in a TaskMap as remote lambda functions, collecting their responses and
        returning them to the caller.

        Args:
            task_map: a TaskMap object holding tasks to be executed asynchronously

    """

    if len(task_map) < 1:
        LOGGER.info('No tasks added')
        return

    response_ids = launch_remote_tasks(task_map)

    if not response_ids:
        LOGGER.info("No responses collected as capture_response=False for all Lambdables. Either you don't care"
                    "about responses from your workers or some error has occurred.")
        return

    total_wait = 0

    # Initial wait period gives the lambdas a period of time to get going before looking for their results
    initial_wait = int(os.getenv('INITIAL_WAIT_SECONDS', 5))
    sleep(initial_wait)
    total_wait += initial_wait

    max_total_wait = int(os.getenv('MAX_TOTAL_WAIT', 900))
    if max_total_wait > 900:
        LOGGER.info('MAX WAIT set to more than 15 minutes (900 seconds) - remote lambda workers can only execute for a '
                    'maximum of 15 minutes, so it is likely that they will start timing out after that time period. If '
                    'expecting remote workers to execute for longer than 15 minutes, consider using a Fargate or Batch '
                    'solution instead')
    num_tasks = len(response_ids)

    if response_ids:
        response_datas = profit(response_ids, total_wait, max_total_wait, num_tasks)
        if len(response_datas) != num_tasks:
            LOGGER.warning('Not all responses collected from tasks, return data will likely be incomplete')

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
            LOGGER.error('Some errors were detected')
        return response_datas
    else:
        LOGGER.warning('No tasks created')


def run_lambdable_task(lambdable: Lambdable):
    if not isinstance(lambdable, Lambdable):
        raise Exception("Can't execute non-Lambdable tasks in lists yet")

    # Generate response ID so that DynamoDB item should have same ID should be the same string as the S3 key
    # that the request will be stored under
    response_id = str(uuid.uuid4())
    if lambdable.request_to_s3:
        # Write args / kwargs to S3 so that lambda worker invoked by run() can retrieve them
        request_key = prep_s3_object(args=lambdable.args,
                                     kwargs=lambdable.kwargs,
                                     key=response_id)
        # Reset args and kwargs so they don't get sent over the wire as they've already been written to S3
        # for retrieval by lambda worker
        lambdable.args = []
        lambdable.kwargs = {}
    else:
        request_key = ''

    # Invoke the remote Lambda function with the arguments provided by the previously defined Lambdable
    response = run(args=lambdable.args,
                   kwargs=lambdable.kwargs,
                   capture_response=lambdable.capture_response,
                   remote_aws_lambda_function_name=lambdable.remote_aws_lambda_func_name,
                   remote_aws_region=lambdable.remote_aws_region,
                   task_path=lambdable.func_path,
                   response_id=response_id,
                   get_request_from_s3=lambdable.request_to_s3,
                   request_s3_bucket=REQUEST_S3_BUCKET,
                   request_s3_key=request_key,
                   response_to_s3=lambdable.response_to_s3
                   )
    return response.response_id


def prep_s3_object(args: Union[tuple, list] = None, kwargs: dict = None, key: str = ''):
    """ Create an object in S3 which holds positional and keyword arguments to be unpacked by a Lambda worker later"""
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}
    if not key:
        key = str(uuid.uuid4())

    s3_client.put_object(Bucket=REQUEST_S3_BUCKET,
                         Body=bytes(json.dumps({'args': args, 'kwargs': kwargs}).encode('utf-8')),
                         Key=key)
    return key


def check_for_errors(response_datas: List[dict]):
    errors = []
    for r in response_datas:
        if r.get('response') == 'N/A' or r.get('status') == 'failed' or 'UnhandledException' in r.get('response', ''):
            errors.append(r)
    return errors


def profit(response_ids: List[str], total_wait: int, max_total_wait: int, num_tasks: int):
    """ Attempt to collect responses from tasks using response IDs returned when launching async lambda functions.

    Args:
        response_ids: List of response IDs corresponding to an item in DynamoDB where the response from a remote task
            will be stored
        total_wait: The amount of time that the manager has already waited for tasks to be finished
        max_total_wait: The maximum time the manager will wait for all responses to be collected before timing out and
            returning what it has
        num_tasks: Number of tasks being executed

    Returns:

    """
    loop_wait = int(os.getenv('LOOP_WAIT_SECONDS', 15))

    response_datas = []
    num_responses_collected = 0

    # While there are still response_ids to collect and time hasn't maxed out, keep trying to get response data from
    # DynamoDB
    while len(response_ids) > 0 and total_wait < max_total_wait:
        LOGGER.info(f'Response IDs: {response_ids}')
        response_datas, response_ids, num_responses_collected = try_getting_responses(response_ids=response_ids,
                                                                                      response_datas=response_datas,
                                                                                      num_responses_collected=num_responses_collected)

        # Not all responses collected yet, sleep for a user-specified amount of time before trying again
        if num_responses_collected != num_tasks:
            LOGGER.info("Didn't get all responses, going to sleep for a bit")
            sleep(loop_wait)
            total_wait += loop_wait
        else:
            # All responses gathered, return them to caller
            return response_datas

    if num_responses_collected == num_tasks:
        LOGGER.info('got all responses!')
        return response_datas
    elif total_wait >= max_total_wait:
        LOGGER.warning('Timed out, returning what responses were collected but data is likely to be incomplete')
        return response_datas


def try_getting_responses(response_ids: List[str],
                          response_datas: List[dict],
                          num_responses_collected: int):
    """ Iterate over task response_ids, using each ID to look up an item in DynamoDB which will store the result of an
        individual lambda task when it has completed. If a response is found for a task, remove its ID from the list so
        it is not checked for again.
    """
    response_ids_collected = []
    for r in response_ids:
        response = get_async_response(r)
        if response is not None:
            # if lambda is still running then just log a message and don't do anything with that ID
            if response.get('status') == 'in progress' and response.get('response') == 'N/A':
                LOGGER.info(f'lambda {r} still going, check back later')
            elif 'fail' in response.get('status', ''):
                response_datas.append(response)
                response_ids_collected.append(r)
                num_responses_collected += 1
            else:
                if 's3_response' in response.get('response', {}) and response.get('response').get('s3_response'):
                    # Get response from S3
                    response_datas.append(get_s3_response(response.get('response')))
                    response_ids_collected.append(r)
                    num_responses_collected += 1
                else:
                    # Response was retrieved from S3, add it to responses that have been collected
                    response_datas.append(response.get('response'))
                    response_ids_collected.append(r)
                    num_responses_collected += 1

    # Remove response_ids that have been collected from responses we're still trying to get
    remove_collected_ids(response_ids_collected, response_ids)

    return response_datas, response_ids, num_responses_collected


def remove_collected_ids(response_ids_collected: List[str], response_ids: List[str]):
    """ Remove any response IDs that have been collected already from the list of response IDs that are being looked
        for
    """
    for r in response_ids_collected:
        try:
            response_ids.remove(r)
        except ValueError:
            pass


def get_s3_response(response: dict):
    """ Retrieve response from worker Lambda which stored its response in S3"""
    s3_bucket = response.get('s3_bucket')
    s3_key = response.get('s3_key')
    LOGGER.info(f'Retrieving data from s3://{s3_bucket}/{s3_key}')
    s3 = boto3.client('s3', region_name='us-west-2')
    return json.loads(s3.get_object(Bucket=s3_bucket, Key=s3_key)['Body'].read().decode('utf-8'))


def remote_runner(*args, **kwargs):
    func = args[0]
    remote_aws_lambda_function_name = kwargs.pop('remote_aws_lambda_function_name', 'remote-phil-dev')
    remote_aws_region = kwargs.pop('remote_aws_region', 'us-west-2')
    capture_response = kwargs.pop('capture_response', True)

    def func_wrapper(func):
        task_path = get_func_task_path(func)
        LOGGER.info(f'Using task path {task_path}')

        @wraps(func)
        @task(remote_aws_lambda_function_name=remote_aws_lambda_function_name,
              remote_aws_region=remote_aws_region,
              capture_response=capture_response,
              task_path=task_path)
        def _run_task(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                import traceback
                logger = logging.getLogger()
                tb = traceback.format_exc()
                logger.error(f'Unhandled exception occurred: {exc}')
                logger.error(f'{tb}')
                return {'UnhandledException': tb}

        update_wrapper(_run_task, func)
        return _run_task

    return func_wrapper(func)
