import json
from api.util.logging import Logging as logger
from api.enums.business.AdapterLogs import BusinessAdapterLogs

def getBusinessName(requester, data):
    if data is not None or len(data) != 0:
        try:
            data = json.loads(data)
            if 'name' in data:
                return data['name']
            else:
                return ''
        except Exception as ex:
            logger.warning(requester, BusinessAdapterLogs.LOGS_GET_BUSINESS_NAME_ERROR.value.format(str(ex)))
            return ''
    return ''


def getDirectorEducational(requester, business_data):
    logger.info(requester, BusinessAdapterLogs.LOGS_START_FUNCTION.value.format(getDirectorEducational.__name__))
    result = ''
    if business_data is not None or len(business_data) != 0:
        try:
            people_data = business_data['people']
            for i in range(len(people_data)):
                if people_data[i]['role'] == 'Director':
                    result = people_data[i]['education']
        except Exception as ex:
            logger.warning(requester, BusinessAdapterLogs.LOGS_GET_DIRECTOR_EDUCATIONAL_ERROR.value.format(str(ex)))
            return ''

    return result
