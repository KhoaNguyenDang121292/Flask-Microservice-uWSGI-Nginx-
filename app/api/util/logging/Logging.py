import logging
import datetime
import configparser
import os.path

from logging.handlers import RotatingFileHandler
from logging import Formatter
from api.core.Directory import getProjectDirectory
from api.core.Directory import createDirectory
from api.core.Directory import createFile

config_path = os.path.realpath(os.path.dirname(__file__)) + '/configuration.ini'
config = configparser.ConfigParser()
config.read(config_path)
log_folder = getProjectDirectory() + config['DEFAULT']['PATH']
log_path = log_folder + str(datetime.date.today()) + '.log'
createDirectory(log_folder)
createFile(log_path)
file_handler = logging.FileHandler(log_path)

logger = logging.getLogger('aspire-logging')
file_handler = RotatingFileHandler(log_path, maxBytes=1000000, backupCount=1)
handler = logging.StreamHandler()


def info(requester, message):
    file_handler.setFormatter(Formatter(
        '%(asctime)s - %(levelname)s: - {0} - %(message)s'.format(requester)
    ))
    handler.setFormatter(Formatter(
        '%(asctime)s - %(levelname)s: - {0} - %(message)s'.format(requester)
    ))
    logger.addHandler(file_handler)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    createDirectory(log_folder)
    logger.info(str(message))

def warning(requester, message):
    file_handler.setFormatter(Formatter(
        '%(asctime)s - %(levelname)s: - {0} - %(message)s'.format(requester)
    ))
    handler.setFormatter(Formatter(
        '%(asctime)s - %(levelname)s: - {0} - %(message)s'.format(requester)
    ))
    logger.addHandler(file_handler)
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    createDirectory(log_folder)
    logger.warning(str(message))

def error(requester, message):
    file_handler.setFormatter(Formatter(
        '%(asctime)s - %(levelname)s: - {0} - %(message)s'.format(requester)
    ))
    handler.setFormatter(Formatter(
        '%(asctime)s - %(levelname)s: - {0} - %(message)s'.format(requester)
    ))
    logger.addHandler(file_handler)
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)
    createDirectory(log_folder)
    logger.error(str(message))