from api.util.logging.SlackLogging import listChannels
from api.util.logging.SlackLogging import channelInfo
from api.util.logging.SlackLogging import sendMessage


def slackCreditBot(message):
    channels = listChannels()
    if channels:
        for channel in channels:
            detailed_info = channelInfo(channel['id'])
            if detailed_info['name'] == 'aspire-credit-logger':
                sendMessage(channel['id'], str(message))
                break
    else:
        print("Unable to authenticate Slack.")
