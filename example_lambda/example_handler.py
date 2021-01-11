from random import randint
from time import sleep

from parallelagram.remote_handler import remote_handler


@remote_handler
def handler(event, context):
    pass

def execute(max_time: int):
    sleep_time = randint(1, max_time)
    print(f'sleeping for {sleep_time} seconds')
    sleep(sleep_time)
    return {'message': f'slept for {sleep_time} seconds'}
