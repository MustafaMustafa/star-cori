"""Function to create a customized logger"""
import logging

def get_logger(name):
    """Returns a logger"""

    formatter = logging.Formatter(fmt='%(asctime)-15s - %(levelname)s - %(module)s : %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger
