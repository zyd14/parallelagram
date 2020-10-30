import os
from time import sleep
from typing import Union, List, Dict, Tuple, Callable, Any
import uuid

import boto3

from parallelagram.utils import LOGGER, prep_s3_object, get_s3_response
from parallelagram.exceptions import EcsTaskConfigurationError, UnableToDetermineContainerName
from parallelagram.zappa_async_fork import run, get_async_response

REQUEST_S3_BUCKET = os.getenv('REQUEST_S3_BUCKET', 'sg-phil-testing')
ecs_client = boto3.client('ecs')


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
        self.s3_args = []
        self.s3_kwargs = {}
        self.response_id = ''  # type: str
        self._response = None
        self.invocation_response = None

    def has_response(self) -> bool:
        if self._response:
            return True

    def get_response(self):
        return self._response

    def run_task(self):
        """ Invoke a remote lambda function specified by the Lambdable object, returning the response_id attribute of a
            DynamoDB item to which the remote lambda function will write its return value to.
        """

        # Generate response ID so that DynamoDB item should have same ID should be the same string as the S3 key
        # that the request will be stored under
        response_id = str(uuid.uuid4())
        if self.request_to_s3:
            # Write args / kwargs to S3 so that lambda worker invoked by run() can retrieve them
            request_key = prep_s3_object(args=self.args,
                                         kwargs=self.kwargs,
                                         key=response_id)
        else:
            request_key = ''

        # Don't send args / kwargs over wire with task invocation if they were written to S3 already
        send_args = [] if self.request_to_s3 else self.args
        send_kwargs = {} if self.request_to_s3 else self.kwargs

        # Invoke the remote Lambda function with the arguments provided by the previously defined Lambdable
        response = run(args=send_args,
                       kwargs=send_kwargs,
                       capture_response=self.capture_response,
                       remote_aws_lambda_function_name=self.remote_aws_lambda_func_name,
                       remote_aws_region=self.remote_aws_region,
                       task_path=self.func_path,
                       response_id=response_id,
                       get_request_from_s3=self.request_to_s3,
                       request_s3_bucket=REQUEST_S3_BUCKET,
                       request_s3_key=request_key,
                       response_to_s3=self.response_to_s3
                       )
        self.invocation_response = response
        self.response_id = response_id

    def try_getting_response(self):
        """ Iterate over task response_ids, using each ID to look up an item in DynamoDB which will store the result of an
            individual lambda task when it has completed. If a response is found for a task, remove its ID from the list so
            it is not checked for again.
        """

        remote_response = get_async_response(self.response_id)
        if remote_response is not None:
            # if lambda is still running then just log a message and don't do anything with that ID
            if remote_response.get('status') == 'in progress' and remote_response.get('response') == 'N/A':
                LOGGER.info(f'lambda with response key {self.response_id} still running, check back later')
            elif 'fail' in remote_response.get('status', ''):
                self._response = remote_response
                return True
            else:
                if 's3_response' in remote_response.get('response', {}) and remote_response.get('response').get('s3_response'):
                    # Get response from S3
                    self._response = get_s3_response(remote_response.get('response'))
                    return True
                else:
                    # Response was retrieved from S3, add it to responses that have been collected
                    self._response = remote_response.get('response')
                    return True


class ResponseCollector:

    def __init__(self, lambdables: List[Lambdable], fail_on_timeout: bool = False):
        self.lambdables = lambdables
        self.num_tasks_completed = 0

        self.max_total_wait = int(os.getenv('MAX_TOTAL_WAIT', 900))
        if self.max_total_wait > 900:
            LOGGER.info(
                'MAX WAIT set to more than 15 minutes (900 seconds) - remote lambda workers can only execute for a '
                'maximum of 15 minutes, so it is likely that they will start timing out after that time period. If '
                'expecting remote workers to execute for longer than 15 minutes, consider using a Fargate or Batch '
                'solution instead')
        self.loop_wait = int(os.getenv('LOOP_WAIT_SECONDS', 15))
        self.fail_on_timeout = fail_on_timeout

    def run_tasks(self):
        """ Launch remote tasks described in Lambdable list"""
        for task in self.lambdables:
            task.run_task()

    def gather_responses(self):
        """ Attempt to collect responses from tasks using response IDs returned when launching async lambda
            functions.
        """

        total_wait = 0
        num_tasks = len(self.lambdables)
        num_responses_collected = 0

        # While there are still response_ids to collect and time hasn't maxed out, keep trying to get response data from
        # DynamoDB
        while num_responses_collected < num_tasks and total_wait < self.max_total_wait:
            for task in self.lambdables:
                if not task.has_response():
                    got_response = task.try_getting_response()
                    if got_response:
                        num_responses_collected += 1

            # Not all responses collected yet, sleep for a user-specified amount of time before trying again
            if num_responses_collected != num_tasks:
                LOGGER.info("Didn't get all responses, going to sleep for a bit")
                sleep(self.loop_wait)
                total_wait += self.loop_wait
            else:
                # All responses gathered, return them to caller
                LOGGER.info('Got all responses')
                return

        if num_responses_collected == num_tasks:
            LOGGER.info('Got all responses')
        elif total_wait >= self.max_total_wait:
            if self.fail_on_timeout:
                error_msg = 'gather_responses timed out while waiting for responses from remote tasks. If the remote ' \
                            'task had not finished yet you may need to increase the max_total_wait. It is also possible' \
                            'that the remote task itself timed out, particularly if it was a Lambda invocation.'
                LOGGER.error(error_msg)
                raise TimeoutError(error_msg)

            LOGGER.warning(
                'Timed out, returning what responses were collected but data is likely to be incomplete')


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