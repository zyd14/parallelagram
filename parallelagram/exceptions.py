from typing import List


class EcsTaskConfigurationError(Exception):
    def __init__(self, errors: List[str], *args, **kwargs):
        super().__init__(str(errors), *args)
        self.errors = errors


class UnableToDetermineContainerName(Exception):
    pass


class TaskTimeoutError(Exception):
    pass


class TaskException(Exception):
    def __init__(self, task_exception: Exception, tb, *args, **kwargs):
        super(task_exception).__init__(task_exception.args[0])
        self.task_exception = task_exception
        self.tb = tb


class ConfigurationException(Exception):
    pass


class AsyncException(Exception):
    """ Simple exception class for async tasks. """
    pass

class NoSuchFunctionFound(Exception):
    """ Exception notifying that user provided a modular path to a function that does not exist"""
    def __init__(self, task_path: str, exc_string: str, *args, **kwargs):
        super().__init__(exc_string)
        self.task_path = task_path
