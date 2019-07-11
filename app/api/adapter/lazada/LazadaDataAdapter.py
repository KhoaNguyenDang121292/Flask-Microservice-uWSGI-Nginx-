import pandas as pd
import numpy as np
from datetime import datetime
from collections import OrderedDict
from pandas.io.json import json_normalize

from api.core.TimeCalculation import monthsdelta

from api.enums.lazada.AdapterLogs import LazadaAdapterLogs

from api.util.logging import Logging as logger


def mapOrdersToDF(requester, business_uuid, data):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(mapOrdersToDF.__name__))
    result_df = pd.DataFrame()
    distinct_columns = ['date', 'code', 'amount', 'currency', 'status', 'cod', 'customer_info']
    if data is not None or len(data) != 0:
        logger.info(requester, LazadaAdapterLogs.LOGS_ORDERS_AVAILABLE.value.format(business_uuid))
        data = data
        for i in range(len(data)):
            obj_row = {}
            obj_row['date'] = datetime.strptime(data[i]['data']['updated_at'][0:10], '%Y-%m-%d')
            obj_row['code'] = data[i]['code']
            obj_row['amount'] = float(str(data[i]['data']['price']).replace(',',''))
            obj_row['currency'] = data[i]['currency']
            obj_row['status'] = data[i]['status']
            obj_row['cod'] = data[i]['data']['payment_method']
            obj_row['customer_info'] = data[i]['data']['address_shipping']['country'] + ' - ' + \
                                 data[i]['data']['address_shipping']['city'] + ' - ' + \
                                 data[i]['data']['address_billing']['last_name'] + ' - ' + \
                                 data[i]['data']['address_billing']['first_name']

            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                         ignore_index=True)
            result_df = result_df.sort_values(by=['date'])
    else:
        logger.warning(requester, LazadaAdapterLogs.LOGS_ORDERS_UNAVAILABLE.value.format(business_uuid))

    return result_df.fillna(0)


def calculateReturnOrdersRatio(requester, ordersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(calculateReturnOrdersRatio.__name__))
    if ordersDF is None or ordersDF.shape[0] == 0 or len(ordersDF) == 0:
        return 0
    try:
        ordersDF['amount'] = ordersDF['amount'].astype(float)
        total_returned = ordersDF[ordersDF['status'] == 'returned'].sum()['amount']
        total_orders = ordersDF['amount'].sum()
        if total_orders == 0:
            return 0
        return total_returned / total_orders
    except Exception as ex:
        logger.warning(requester, LazadaAdapterLogs.LOGS_RETURNED_ORDERS_RATIO_ERROR.value.format(str(ex)))
        return 0


def calculateCancelledOrdersRatio(requester, ordersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(calculateCancelledOrdersRatio.__name__))
    if ordersDF is None or ordersDF.shape[0] == 0 or len(ordersDF) == 0:
        return 0
    try:
        ordersDF['amount'] = ordersDF['amount'].astype(float)
        total_returned = ordersDF[ordersDF['status'] == 'canceled'].sum()['amount']
        total_orders = ordersDF['amount'].sum()
        if total_orders == 0:
            return 0
        return total_returned / total_orders
    except Exception as ex:
        logger.warning(requester, LazadaAdapterLogs.LOGS_CANCELLED_ORDERS_RATIO_ERROR.value.format(str(ex)))
        return 0


def calculateNumberOfCust(requester, ordersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(calculateNumberOfCust.__name__))
    if ordersDF is None or ordersDF.shape[0] == 0 or len(ordersDF) == 0:
        return 0
    try:
        ordersDF['amount'] = ordersDF['amount'].astype(float)
        total_custs = ordersDF[ordersDF['status'] == 'delivered'].count()[0]
        return total_custs
    except Exception as ex:
        logger.warning(requester, LazadaAdapterLogs.LOGS_NUMBER_OF_CUSTOMERS_ERROR.value.format(str(ex)))
        return 0

def getFirstOrdersDate(requester, ordersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getFirstOrdersDate.__name__))
    if ordersDF is None or ordersDF.shape[0] == 0 or len(ordersDF) == 0:
        return ''
    try:
        return ordersDF.iloc[0]['date'].strftime("%Y-%m-%d")
    except Exception as ex:
        logger.warning(requester, LazadaAdapterLogs.LOGS_FIRST_ORDER_DATE_ERROR.value.format(str(ex)))
        return ''

def getMonthSinceFirstOrder(requester, firstOrderDate):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getMonthSinceFirstOrder.__name__))
    if firstOrderDate is not None or firstOrderDate != "":
        try:
            current_date = datetime.now().strftime('%Y-%m-%d')
            return monthsdelta(firstOrderDate, current_date)
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_MONTH_SINCE_FIRST_ORDER_ERROR.value.format(str(ex)))
            return ''
    return ''

def getMonthlyRevenueDF(requester, ordersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getMonthlyRevenueDF.__name__))
    if ordersDF is None or ordersDF.shape[0] == 0 or len(ordersDF) == 0:
        return pd.DataFrame()
    try:
        ordersDF['amount'] = ordersDF['amount'].astype(float)
        ordersDF = ordersDF[ordersDF['status'] == 'delivered']
        monthlyRevDF = ordersDF.groupby(ordersDF['date'].dt.strftime('%Y-%m'))['amount'].sum().sort_values()
        currencyDF = ordersDF.groupby(ordersDF['date'].dt.strftime('%Y-%m'))['currency'].unique()
        result_df = pd.concat([monthlyRevDF, currencyDF], ignore_index=True, axis=1, sort=True)
        monthlyTransDF = ordersDF.groupby(ordersDF['date'].dt.strftime('%Y-%m'))['amount'].count().sort_values()
        result_df = pd.concat([result_df, monthlyTransDF], ignore_index=True, axis=1, sort=True)
        result_df.columns = ['amount', 'currency', 'count']
        return result_df
    except Exception as ex:
        logger.warning(requester, LazadaAdapterLogs.LOGS_MONTHLY_REVENUE_ERROR.value.format(str(ex)))
        return pd.DataFrame()


def getOrdersRawDF(requester, data):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getOrdersRawDF.__name__))
    if data is not None or len(data) != 0:
        try:
            raw_df = json_normalize(data)
            del raw_df['amount']
            return raw_df
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_ORDERS_RAW_DF_ERROR.value.format(str(ex)))
            return pd.DataFrame()

    return pd.DataFrame()

def mapSellersRawDF(requester, data):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(mapSellersRawDF.__name__))
    if data is not None or len(data) != 0:
        try:
            raw_df = json_normalize(data)
            del raw_df['amount']
            return raw_df
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_SELLERS_RAW_DF_ERROR.value.format(str(ex)))
            return pd.DataFrame()

    return pd.DataFrame()


def mapSellerMetricsRawDF(requester, data):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(mapSellerMetricsRawDF.__name__))
    if data is not None or len(data) != 0:
        try:
            raw_df = json_normalize(data)
            del raw_df['amount']
            return raw_df
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_SELLERS_METRICS_ERROR.value.format(str(ex)))
            return pd.DataFrame()

    return pd.DataFrame()


def getSellerMetricsPositiveSellRating(requester, sellersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(mapSellerMetricsRawDF.__name__))
    if sellersDF is not None or sellersDF.shape[0] != 0 or len(sellersDF) != 0:
        try:
            return float(sellersDF['data.positive_seller_rating'][0])/float(100)
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_POSITIVE_SELL_RATING_ERROR.value.format(str(ex)))
            return 0
    return 0


def getSellerMetricsShipOnTime(requester, sellersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getSellerMetricsShipOnTime.__name__))
    result = 0
    if sellersDF is not None or sellersDF.shape[0] != 0 or len(sellersDF) != 0:
        try:
            result = float(sellersDF['data.ship_on_time'][0])/float(100)
            if result == 'null':
                return 0
            return result
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_SHIP_ON_TIME_ERROR.value.format(str(ex)))
            return 0
    return result


def getSellerMetricsResponseTime(requester, sellersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getSellerMetricsResponseTime.__name__))
    result = 0
    if sellersDF.shape[0] != 0 and len(sellersDF) != 0:
        try:
            result = sellersDF['data.response_time'][0]
            if result == 'null':
                return 0
            return result
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_RESPONSE_TIME_ERROR.value.format(str(ex)))
            return 0
    return result


def getSellerMetricsResponseRate(requester, sellersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getSellerMetricsResponseRate.__name__))
    result = 0
    if sellersDF.shape[0] != 0 and len(sellersDF) != 0:
        try:
            result = sellersDF['data.response_rate'][0]
            if result == 'null':
                return 0
            return result
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_RESPONSE_RATE_ERROR.value.format(str(ex)))
            return 0
    return result


def getSellerMetricsMainCategoryName(requester, sellersDF):
    logger.info(requester,
                LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getSellerMetricsMainCategoryName.__name__))
    if sellersDF is not None or sellersDF.shape[0] != 0 or len(sellersDF) != 0:
        try:
            return sellersDF['data.main_category_name'][0]
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_MAIN_CATEGORY_ERROR.value.format(str(ex)))
            return ''
    return ''


def mapCategoryTreeRawDF(requester, data):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(mapCategoryTreeRawDF.__name__))
    if data is not None or len(data) != 0:
        try:
            raw_df = json_normalize(data)
            del raw_df['amount']
            return raw_df
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_MAP_CATEGORY_TREE_ERROR.value.format(str(ex)))
            return pd.DataFrame()
    return pd.DataFrame()


def mapProductItemsRawDF(requester, data):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(mapProductItemsRawDF.__name__))
    if data is not None or len(data) != 0:
        try:
            raw_df = json_normalize(data)
            del raw_df['amount']
            return raw_df
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_MAP_PRODUCT_ITEMS_ERROR.value.format(str(ex)))
            return pd.DataFrame()
    return pd.DataFrame()


def mapTransDetailsRawDF(requester, data):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(mapTransDetailsRawDF.__name__))
    if data is not None or len(data) != 0:
        try:
            raw_df = json_normalize(data)
            del raw_df['amount']
            return raw_df
        except Exception as ex:
            logger.warning(requester, LazadaAdapterLogs.LOGS_MAP_TRANSATION_DETAIL_ERROR.value.format(str(ex)))
            return pd.DataFrame()
    return pd.DataFrame()


def getAvgTransaction(requester, ordersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getAvgTransaction.__name__))
    if ordersDF is None or ordersDF.shape[0] == 0 or len(ordersDF) == 0:
        return 0
    try:
        ordersDF = ordersDF[ordersDF['status']=='delivered']
        monthlyTransDF = ordersDF.groupby(ordersDF['date'].dt.strftime('%Y-%m'))['amount'].count().sort_values()
        avgTrans = monthlyTransDF.mean()
        return avgTrans
    except Exception as ex:
        logger.warning(requester, LazadaAdapterLogs.LOGS_MAP_AVG_TRANSATION_ERROR.value.format(str(ex)))
        return 0
    return 0


def getRevVol(requester, df):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getRevVol.__name__))
    try:
        if df.loc[:,"amount"].mean() != 0 or df.loc[:,"amount"].mean() is not None:
            return df.loc[:,"amount"].std() / df.loc[:,"amount"].mean()
    except Exception as ex:
        logger.warning(requester, LazadaAdapterLogs.LOGS_GET_REVENUE_VOLATILITY_ERROR.value.format(str(ex)))
        return 0
    return 0

def getMonthlyEcommerceSales(inputMonthlyRevenueDF):
    result = 0
    if inputMonthlyRevenueDF is not None or inputMonthlyRevenueDF.shape[0] != 0:
        try:
            result = inputMonthlyRevenueDF.loc[:,"amount"].mean()
        except Exception as ex:
            return 0
    return result


def getAnnualEcommerceSales(inputMonthlyRevenueDF):
    result = 0
    if inputMonthlyRevenueDF is not None or inputMonthlyRevenueDF.shape[0] != 0:
        try:
            result = float(round(inputMonthlyRevenueDF.loc[:, "amount"].mean(), 4))*12
        except Exception as ex:
            return 0
    return result


def getSellerStoreName(sellersDF):
    resultDF = ''
    if sellersDF is not None or sellersDF.shape[0] != 0:
        try:
            resultDF = str(sellersDF['data.name'][0]).replace(" ", "-")
        except Exception as ex:
            return ''
    return resultDF


def getShopURL(sellersDF):
    resultDF = ''
    if sellersDF is not None or sellersDF.shape[0] != 0:
        try:
            resultDF = str(sellersDF['data.name'][0]).replace(" ", "-")
        except Exception as ex:
            return ''
    return resultDF


def generateDataPoints(requester, inputMonthlyRevenueDF, avgMonthlyOrdersDF,
                       ordersDF, educationLevel, sellerMetricsDF, sellerDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(generateDataPoints.__name__))
    if inputMonthlyRevenueDF is None or inputMonthlyRevenueDF.shape[0] == 0 or len(inputMonthlyRevenueDF) == 0:
        return pd.DataFrame()
    datapointDF = pd.DataFrame()
    dataPointsDict = OrderedDict()
    try:
        dataPointsDict['monthly_ecommerce_sales'] = round(getMonthlyEcommerceSales(inputMonthlyRevenueDF), 4)
        dataPointsDict['annual_ecommerce_sales'] = getAnnualEcommerceSales(inputMonthlyRevenueDF)
        dataPointsDict['monthly_delivered_orders'] = round(avgMonthlyOrdersDF, 4)
        dataPointsDict['revenue_volatility'] = round(getRevVol(requester, inputMonthlyRevenueDF), 4)
        dataPointsDict['returned_order_ratio'] = round(calculateReturnOrdersRatio(requester, ordersDF), 4)
        dataPointsDict['canceled_order_ratio'] = round(calculateCancelledOrdersRatio(requester, ordersDF), 4)
        dataPointsDict['number_of_customer'] = round(calculateNumberOfCust(requester, ordersDF), 4)
        firstOrderDate = getFirstOrdersDate(requester, ordersDF)
        dataPointsDict['first_date_delivered'] = firstOrderDate
        dataPointsDict['month_since_first_order'] = getMonthSinceFirstOrder(requester, firstOrderDate)
        dataPointsDict['education_level'] = educationLevel
        dataPointsDict['positive_sell_rating'] = getSellerMetricsPositiveSellRating(requester, sellerMetricsDF)
        dataPointsDict['ship_on_time'] = getSellerMetricsShipOnTime(requester, sellerMetricsDF)
        dataPointsDict['response_time'] = getSellerMetricsResponseTime(requester, sellerMetricsDF)
        dataPointsDict['response_rate'] = getSellerMetricsResponseRate(requester, sellerMetricsDF)
        dataPointsDict['main_category_name'] = getSellerMetricsMainCategoryName(requester, sellerMetricsDF)
        dataPointsDict['seller_store_name'] = getSellerStoreName(sellerDF)
        dataPointsDict['lazada_shop_url'] = getShopURL(sellerDF)

        for ind in dataPointsDict:
            datapointDF = pd.concat([datapointDF, pd.DataFrame([ind , dataPointsDict[ind]],
                                                               index = ['data_point', 'value']).T], sort=True)

        datapointDF = datapointDF.set_index('data_point')
        datapointDF.fillna(0)
        return datapointDF
    except Exception as ex:
        logger.warning(requester, LazadaAdapterLogs.LOGS_GENERATE_DATA_POINTS_ERROR.value.format(str(ex)))
        return pd.DataFrame()
    return pd.DataFrame()


def getPaymentMethodDF(requester, ordersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getPaymentMethodDF.__name__))
    if ordersDF is None or ordersDF.shape[0] == 0 or len(ordersDF) == 0:
        return pd.DataFrame()
    try:
        ordersDF = ordersDF[ordersDF['status'] == 'delivered']
        monthlyTransactionsDF = ordersDF.groupby(ordersDF['cod'])['amount'].count()
        total_count = ordersDF.groupby(ordersDF['cod'])['amount'].count().sum()
        paymentMethodDF = ordersDF.groupby(ordersDF['cod'])['amount'].count().apply(lambda x: x/total_count)
        result_df = pd.concat([monthlyTransactionsDF, paymentMethodDF], ignore_index=True, axis=1, sort=True)
        result_df.columns = ['count', 'percentage']
        result_df = result_df.sort_values(by=['count'], ascending=False)
        return result_df
    except Exception as ex:
        logger.warning(requester, LazadaAdapterLogs.LOGS_GET_PAYMENT_METHOD_ERROR.value.format(str(ex)))
        return pd.DataFrame()
    return pd.DataFrame()


def getUniqueCustomers(requester, ordersDF):
    logger.info(requester, LazadaAdapterLogs.LOGS_START_FUNCTION.value.format(getUniqueCustomers.__name__))
    if ordersDF is None or ordersDF.shape[0] == 0 or len(ordersDF) == 0:
        return pd.DataFrame()
    try:
        ordersDF['amount'].astype(float)
        uniqueCustomersDF = ordersDF.groupby('customer_info')['amount'].agg([np.sum])
        currencyDF = ordersDF.groupby(ordersDF['customer_info'])['currency'].unique()
        result_df = pd.concat([uniqueCustomersDF, currencyDF], axis=1, sort=True)
        total_amount = uniqueCustomersDF['sum'].sum()
        percentageDF = ordersDF.groupby('customer_info')['amount'].agg([np.sum]).apply(lambda x: x/total_amount)
        result_df = pd.concat([result_df, percentageDF], axis=1, sort=True)
        result_df.columns = ['amount', 'currency', 'percentage']
        result_df = result_df.sort_values(by=['amount'], ascending=False)
        return result_df
    except Exception as ex:
        logger.warning(requester, LazadaAdapterLogs.LOGS_GET_UNIQUE_CUSTOMERS_ERROR.value.format(str(ex)))
        return pd.DataFrame()
    return pd.DataFrame()