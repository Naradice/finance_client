import datetime
import os
from logging import config, getLogger

__logger = getLogger("fprocess")


def info(message: str, **kwargs):
    try:
        __logger.info(message, **kwargs)
    except Exception:
        print(message)


def warning(message: str, **kwargs):
    try:
        __logger.warning(message, **kwargs)
    except Exception:
        print(message)


def warn(message: str, **kwargs):
    warning(message, **kwargs)


def error(message: str, **kwargs):
    try:
        __logger.error(message, **kwargs)
    except Exception:
        print(message)


def exception(message: str, **kwargs):
    try:
        __logger.exception(message, **kwargs)
    except Exception:
        print(message)


def critical(message, **kwargs):
    try:
        __logger.critical(message, **kwargs)
    except Exception:
        print(message)


def debug(message: str, **kwargs):
    try:
        __logger.debug(message, **kwargs)
    except Exception:
        print(message)
