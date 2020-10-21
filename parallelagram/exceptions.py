from typing import List


class EcsTaskConfigurationError(Exception):

    def __init__(self, errors: List[str], *args, **kwargs):
        super().__init__(str(errors), *args)
        self.errors = errors


class UnableToDetermineContainerName(Exception):
    pass