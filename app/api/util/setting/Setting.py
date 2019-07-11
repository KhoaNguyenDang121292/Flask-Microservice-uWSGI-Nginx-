import os.path
import configparser


config_path = os.path.realpath(os.path.dirname(__file__)) + '/configuration.ini'
config = configparser.ConfigParser()
config.read(config_path)


def init(token):
    settings = {}

    if config['DEFAULT']['APP_ENV_TYPE'] == 'PRODUCT':
        settings['APP_URL'] = config['DEFAULT']['APP_PRODUCT_URL']
    if config['DEFAULT']['APP_ENV_TYPE'] == 'STAGING':
        settings['APP_URL'] = config['DEFAULT']['APP_STAGING_URL']
    
    headers = {'Authorization': 'Bearer ' + str(token), 'X-Requested-With': 'XMLHttpRequest'}
    settings['ASPIRE_API_TOKEN'] = headers

    return settings

