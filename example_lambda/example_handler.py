from random import randint
from time import sleep


def handler(event, context):
    sleep_time = randint(1, 10)
    print(f'sleeping for {sleep_time} seconds')
    sleep(sleep_time)
    return {'message': f'slept for {sleep_time} seconds'}
