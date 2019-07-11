import pandas as pd
from datetime import datetime
from collections import OrderedDict
from pandas.io.json import json_normalize
from api.enums.moka.AdapterLogs import MokaAdapterLogs
from api.util.logging import Logging as logger


def mapOrdersToDF(requester, business_uuid, data):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(mapOrdersToDF.__name__))
    resultDF = pd.DataFrame()
    data = data['data']
    for item in range(0, len(data)):
        rowList = []
        row = OrderedDict()
        row['code'] = data[item]['code']
        dateStr = data[item]['data']['updated_at'][0:10]
        row['date'] = datetime.strptime(dateStr, '%Y-%m-%d')

        dataNode = data[item]['data']
        for i in range(1, len(dataNode)):
            row['amount'] = float(dataNode['total_price'])
            row['status'] = dataNode['financial_status']
            # if itemHeader in nameMappingDict.keys():
            #    row[nameMappingDict[itemHeader]] = float(itemValue)
            # else:
            #    print('\33[31m','ERROR::mapXeroPnLToDF','Name' + itemHeader +' has been changed, Please trace back to JSON data to reassure')
            #    print('\33[31m','ERROR::mapXeroPnLToDF',e)
        # print(row)
        rowList.append(row)
        resultDF = resultDF.append(rowList, ignore_index=True, sort=True)
    resultDF.where(resultDF.notnull(), 0)
    return resultDF.fillna(0)


def mapCountriesToDF(requester, business_uuid, data):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(mapCountriesToDF.__name__))
    resultDF = pd.DataFrame()
    data = data['data']
    for item in range(0, len(data)):
        rowList = []
        row = OrderedDict()
        row['code'] = data[item]['code']
        dataNode = data[item]['data']
        for i in range(1, len(dataNode)):
            row['name'] = dataNode['name']
            row['tax_name'] = dataNode['tax_name']
            row['provinces'] = dataNode['provinces']
            # if itemHeader in nameMappingDict.keys():
            #    row[nameMappingDict[itemHeader]] = float(itemValue)
            # else:
            #    print('\33[31m','ERROR::mapXeroPnLToDF','Name' + itemHeader +' has been changed, Please trace back to JSON data to reassure')
            #    print('\33[31m','ERROR::mapXeroPnLToDF',e)
        # print(row)
        rowList.append(row)
        resultDF = resultDF.append(rowList, ignore_index=True, sort=True)
    resultDF.where(resultDF.notnull(), 0)
    return resultDF.fillna(0)


def mapShopInfoToDF(requester, business_uuid, data):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(mapShopInfoToDF.__name__))
    resultDF = pd.DataFrame()
    data = data['data']
    for item in range(0, len(data)):
        rowList = []
        row = OrderedDict()
        row['code'] = data[item]['code']
        dateStr = data[item]['data']['updated_at'][0:10]
        row['date'] = datetime.strptime(dateStr, '%Y-%m-%d')

        dataNode = data[item]['data']
        for i in range(1, len(dataNode)):
            row['amount'] = float(dataNode['total_price'])
            row['status'] = dataNode['financial_status']
            # if itemHeader in nameMappingDict.keys():
            #    row[nameMappingDict[itemHeader]] = float(itemValue)
            # else:
            #    print('\33[31m','ERROR::mapXeroPnLToDF','Name' + itemHeader +' has been changed, Please trace back to JSON data to reassure')
            #    print('\33[31m','ERROR::mapXeroPnLToDF',e)
        # print(row)
        rowList.append(row)
        resultDF = resultDF.append(rowList, ignore_index=True, sort=True)
    resultDF.where(resultDF.notnull(), 0)
    return resultDF.fillna(0)


def mapCustomersToDF(requester, business_uuid, data):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(mapCustomersToDF.__name__))
    resultDF = pd.DataFrame()
    data = data['data']
    for item in range(0, len(data)):
        rowList = []
        row = OrderedDict()
        row['code'] = data[item]['code']
        dateStr = data[item]['data']['updated_at'][0:10]
        row['date'] = datetime.strptime(dateStr, '%Y-%m-%d')

        dataNode = data[item]['data']
        for i in range(1, len(dataNode)):
            row['amount'] = float(dataNode['total_price'])
            row['status'] = dataNode['financial_status']
            # if itemHeader in nameMappingDict.keys():
            #    row[nameMappingDict[itemHeader]] = float(itemValue)
            # else:
            #    print('\33[31m','ERROR::mapXeroPnLToDF','Name' + itemHeader +' has been changed, Please trace back to JSON data to reassure')
            #    print('\33[31m','ERROR::mapXeroPnLToDF',e)
        # print(row)
        rowList.append(row)
        resultDF = resultDF.append(rowList, ignore_index=True, sort=True)
    resultDF.where(resultDF.notnull(), 0)
    return resultDF.fillna(0)


def getMonthlyRevenueDF(requester, business_uuid, ordersDF):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(getMonthlyRevenueDF.__name__))
    result_df = pd.DataFrame()
    if ordersDF is not None and ordersDF.shape[0] != 0:
        try:
            monthlyRevDF = ordersDF.groupby(ordersDF['date'].dt.strftime('%B'))['amount'].sum().sort_values()
            result_df = monthlyRevDF.to_frame()
        except Exception as ex:
            return pd.DataFrame()

    return result_df


def getRevVol(requester, business_uuid, df):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(getRevVol.__name__))
    result = 0
    try:
        result = df.loc[:, "amount"].std() / df.loc[:, "amount"].mean()
    except Exception as ex:
        return 0

    return result


def generateDataPoints(requester, business_uuid, inputMonthlyRevenueDF):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(generateDataPoints.__name__))
    datapointDF = pd.DataFrame()
    if inputMonthlyRevenueDF is not None and inputMonthlyRevenueDF.shape[0] != 0:
        dataPointsDict = OrderedDict()
        dataPointsDict['monthly_avg_revenue'] = round(inputMonthlyRevenueDF.loc[:, "amount"].mean())
        dataPointsDict['revenue_volatility'] = getRevVol(inputMonthlyRevenueDF)
        for ind in dataPointsDict:
            datapointDF = pd.concat(
                [datapointDF, pd.DataFrame([ind, dataPointsDict[ind]], index=['data_point', 'value']).T], sort=True)

    return datapointDF


def getBusinessInfoRawDF(requester, data):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(getBusinessInfoRawDF.__name__))
    result_df = pd.DataFrame()
    if data is not None:
        try:
            raw_df = json_normalize(data['data'])
            del raw_df['amount']
            result_df = raw_df
        except Exception as ex:
            return pd.DataFrame()

    return result_df


def getTransactionsRawDF(requester, data):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(getTransactionsRawDF.__name__))
    result_df = pd.DataFrame()
    if data is not None:
        try:
            raw_df = json_normalize(data['data'])
            del raw_df['amount']
            result_df = raw_df
        except Exception as ex:
            return pd.DataFrame()

    return result_df


def getOutletsRawDF(requester, data):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(getOutletsRawDF.__name__))
    result_df = pd.DataFrame()
    if data is not None:
        try:
            raw_df = json_normalize(data['data'])
            del raw_df['amount']
            result_df = raw_df
        except Exception as ex:
            return pd.DataFrame()

    return result_df


def mapTransactionDF(requester, data):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(mapTransactionDF.__name__))
    result_df = pd.DataFrame()
    if data is not None:
        data = data['data']
        distinct_columns = ['date', 'code', 'amount', 'currency', 'cod', 'contact', 'status']
        for i in range(len(data)):
            obj_row = {}
            if i == 0:
                continue
            try:
                obj_row['date'] = data[i]['data']['created_at'][:10]
                obj_row['code'] = data[i]['data']['payment_no']
                obj_row['amount'] = data[i]['data']['total_collected']
                obj_row['currency'] = data[i]['currency']
                obj_row['cod'] = data[i]['data']['payment_type']
                obj_row['contact'] = data[i]['contact']
                obj_row['status'] = data[i]['status']
            except Exception as ex:
                continue

            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                         ignore_index=True)

        result_df['date'] = pd.to_datetime(result_df['date'])

    return result_df.fillna(0)


def mapMonthlyRevenueDF(requester, transactionDF):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(mapTransactionDF.__name__))
    result_df = pd.DataFrame()
    if transactionDF is not None and transactionDF.shape[0] == 0 and len(transactionDF) == 0:
        try:
            transactionDF['amount'] = transactionDF['amount'].astype(float)
            # ordersDF = ordersDF[ordersDF['status'] == 'delivered']
            monthlyRevDF = transactionDF.groupby(transactionDF['date'].dt.strftime('%Y-%m'))['amount'].sum().sort_values()
            currencyDF = transactionDF.groupby(transactionDF['date'].dt.strftime('%Y-%m'))['currency'].unique()
            result_df = pd.concat([monthlyRevDF, currencyDF], ignore_index=True, axis=1, sort=True)
            monthlyTransDF = transactionDF.groupby(transactionDF['date'].dt.strftime('%Y-%m'))['amount'].count().sort_values()
            result_df = pd.concat([result_df, monthlyTransDF], ignore_index=True, axis=1, sort=True)
            result_df.columns = ['amount', 'currency', 'count']
            return result_df
        except Exception as ex:
            print("ERROR - Get Monthly Revenue failed. Error - " + str(ex))
            return pd.DataFrame()

    return result_df


def getMonthlyAvgRevenue(requester, transactionDF):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(getMonthlyAvgRevenue.__name__))
    result = 0
    try:
        result = round(transactionDF.loc[:, "amount"].mean(), 4)
    except Exception as ex:
        return 0

    return result


def generateDataPoints(requester, transactionDF):
    logger.info(requester, MokaAdapterLogs.LOGS_START_FUNCTION.value.format(generateDataPoints.__name__))
    if transactionDF is None or transactionDF.shape[0] == 0 or len(transactionDF) == 0:
        return pd.DataFrame()
    datapointDF = pd.DataFrame()
    dataPointsDict = OrderedDict()
    try:
        dataPointsDict['avg_monthly_revenue'] = getMonthlyAvgRevenue(transactionDF)
        dataPointsDict['revenue_volatility'] = getRevVol(transactionDF)

        for ind in dataPointsDict:
            datapointDF = pd.concat(
                [datapointDF, pd.DataFrame([ind, dataPointsDict[ind]], index=['data_point', 'value']).T], sort=True)

        datapointDF = datapointDF.set_index('data_point')
        return datapointDF
    except Exception as ex:
        print("ERROR - generate data points failed - " + str(ex))
        return pd.DataFrame()
