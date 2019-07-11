import os
from slackclient import SlackClient
import configparser


config_path = os.path.realpath(os.path.dirname(__file__)) + '/configuration.ini'
config = configparser.ConfigParser()
config.read(config_path)

slack_client = SlackClient(config['SLACK']['TOKEN'])

def listChannels():
    channels_call = slack_client.api_call("channels.list")
    if channels_call.get('ok'):
        return channels_call['channels']
    return None

def channelInfo(channel_id):
    channel_info = slack_client.api_call("channels.info", channel=channel_id)
    if channel_info:
        return channel_info['channel']
    return None

def sendMessage(channel_id, message):
    slack_client.api_call(
        "chat.postMessage",
        channel=channel_id,
        text=message,
        username='Log Bot',
        icon_emoji=':troller:'
    )
