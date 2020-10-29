import logging


def create_logger() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel('INFO')
    sh = logging.StreamHandler()
    sh.setLevel('INFO')
    logger.addHandler(sh)
    return logger


LOGGER = create_logger()