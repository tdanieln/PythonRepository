import logging
from logging.handlers import RotatingFileHandler
import os

__author__ = 'tdanieln@gmail.com'

LOG_DIR = './logs'
LOG_PATH_FILE = "./logs/etl.log"
LOG_MODE = 'a'
LOG_MAX_SIZE = 50*1024*1024
LOG_MAX_FILES = 50
LOG_LEVEL = logging.DEBUG

if os.path.exists(LOG_DIR) is False:
    os.mkdir(LOG_DIR)

file_handler = RotatingFileHandler(LOG_PATH_FILE,LOG_MODE, LOG_MAX_SIZE, LOG_MAX_FILES)
file_handler.setLevel(logging.DEBUG)

std_hanlder = logging.StreamHandler()
std_hanlder.setLevel(logging.INFO)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
std_hanlder.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(std_hanlder)

logger_std = logging.getLogger()
logger_std.setLevel(logging.DEBUG)
logger_std.addHandler(std_hanlder)


def debug(content):
    logger_std.debug(content)


def error(content):
    logger.error(content)


def get_logger():
    return logger
