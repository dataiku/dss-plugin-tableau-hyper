"""
    General type casting functions common to hyper to dss or dss to hyper.
    Any specific functions for type casting should be written directly in the classes themselves.
"""

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Tableau Plugin | %(levelname)s - %(message)s')


def enforce_int(value):
    """
        Enforce int format on value
    :param value:
    :return:
    """
    if value is not None:
        return int(value)
    else:
        return None


def enforce_float(value):
    """
        Enforce float format on value
    :param value:
    :return:
    """
    if value is not None:
        return float(value)
    else:
        return None


def enforce_bool(value):
    """
    Enforce boolean type
    :param value:
    :return:
    """
    if value is not None:
        return bool(value)
    else:
        return False


def enforce_string(value):
    """
        Enforce string format on value
    :param value:
    :return:
    """
    if type(value) is not str:
        logger.info("Invalid format of value. Expected string: {}".format(value))
    if value is not None:
        return str(value)
    else:
        return str()