import pandas as pd
from datetime import datetime
from collections import OrderedDict
from pandas import ExcelWriter
from pandas.io.json import json_normalize
import numpy as np
from api.core.TimeCalculation import monthsdelta


def exportRawXLSX(business_name, customers_data, shops_data):
    try:
        raw_customers_df = json_normalize(customers_data['data'])
        raw_shops_df = json_normalize(shops_data['data'])
        writer = ExcelWriter('RawData_' + business_name + '.xlsx')
        raw_customers_df.to_excel(writer, 'Customers')
        raw_shops_df.to_excel(writer, 'Shops')
        writer.save()
    except Exception as ex:
        print("ERROR when export raw data (Customers and Shops) for business " + business_name + " error: " + str(ex))

    print("Export raw (Customers and Shops) for business " + business_name + " is successfully.")


def mapOrdersToDF(data):
    resultDF = pd.DataFrame()
    data = data['data']
    for item in range(0,len(data)):     
        rowList = []
        row = OrderedDict()
        row['code'] = data[item]['code']
        dateStr = data[item]['data']['updated_at'][0:10]
        row['date'] = datetime.strptime(dateStr, '%Y-%m-%d')
        
        dataNode = data[item]['data']
        for i in range(1, len(dataNode)):
            row['amount'] = float(dataNode['total_price'])
            row['status'] = dataNode['financial_status']
            #if itemHeader in nameMappingDict.keys():
            #    row[nameMappingDict[itemHeader]] = float(itemValue)
            #else:
            #    print('\33[31m','ERROR::mapXeroPnLToDF','Name' + itemHeader +' has been changed, Please trace back to JSON data to reassure')
            #    print('\33[31m','ERROR::mapXeroPnLToDF',e)
        #print(row)
        rowList.append(row)
        resultDF = resultDF.append(rowList, ignore_index=True, sort = True)
    resultDF.where(resultDF.notnull(), 0)
    return resultDF.fillna(0)


def mapCountriesToDF(data):
    resultDF = pd.DataFrame()
    data = data['data']
    for item in range(0,len(data)):     
        rowList = []
        row = OrderedDict()
        row['code'] = data[item]['code']
        dataNode = data[item]['data']
        for i in range(1, len(dataNode)):
            row['name'] = dataNode['name']
            row['tax_name'] = dataNode['tax_name']
            row['provinces'] = dataNode['provinces']
            #if itemHeader in nameMappingDict.keys():
            #    row[nameMappingDict[itemHeader]] = float(itemValue)
            #else:
            #    print('\33[31m','ERROR::mapXeroPnLToDF','Name' + itemHeader +' has been changed, Please trace back to JSON data to reassure')
            #    print('\33[31m','ERROR::mapXeroPnLToDF',e)
        #print(row)
        rowList.append(row)
        resultDF = resultDF.append(rowList, ignore_index=True, sort = True)
    resultDF.where(resultDF.notnull(), 0)
    return resultDF.fillna(0)

def mapShopInfoToDF(data):
    resultDF = json_normalize(data['data'])
    return resultDF

def mapCustomersToDF(data):
    data = data['data']
    distinct_columns = ['contact', 'total spent', 'status']
    result_df = pd.DataFrame()
    if data is not None or len(data) != 0:
        obj_row = {}
        for i in range(len(data)):
            try:
                obj_row['contact'] = str(data[i]['contact']) + ' - ' + str(data[i]['data']['default_address']['zip']) + ' - ' \
                          + str(data[i]['data']['default_address']['address2'])
                obj_row['total spent'] = data[i]['data']['total_spent']
                obj_row['status'] = data[i]['status']
            except Exception as ex:
                continue

            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                        ignore_index=True)
    else:
        print("INFO - The customers data is not available.")
    return result_df.fillna(0)


def mapOrdersToDF(data):
    result_df = pd.DataFrame()
    distinct_columns = ['customer_name', 'amount', 'code', 'date', 'status']

    if data is not None or len(data['data']) != 0:
        data = data['data']
        for i in range(len(data)):
            obj_row = {}
            obj_row['customer_name'] = data[i]['data']['customer']['first_name'] + ' ' + data[i]['data']['customer']['last_name']
            obj_row['amount'] = data[i]['amount']
            obj_row['code'] = data[i]['code']
            obj_row['date'] = data[i]['data']['updated_at'][0:10]
            obj_row['status'] = data[i]['status']
            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                            ignore_index=True)
    else:
        print("INFO - The customers data is not available.")

    return result_df.fillna(0)


def getCustomerAmount(customersDF):
    if customersDF is None or customersDF.shape[0] == 0 or len(customersDF) == 0:
        return pd.DataFrame()
    try:
        customersDF['total spent'] = customersDF['total spent'].astype('float64')
        customerCountDF = customersDF.groupby('contact')['total spent'].agg([np.sum])
        total_count = customerCountDF['sum'].sum()
        percentageDF = customersDF.groupby('contact')['total spent'].agg([np.sum]).apply(lambda x: x * 100 / total_count)
        result_df = pd.concat([customerCountDF, percentageDF], ignore_index=True, axis=1)
        result_df.columns = ['amount', 'percentage']
        result_df = result_df.sort_values(by='amount', ascending=True)
        return result_df
    except Exception as ex:
        print("ERROR - Get Customer Amount failed - " + str(ex))
        return pd.DataFrame()

def getNumberOfCustomers(customersDF):
    result = 0
    if customersDF is not None or customersDF.shape[0] != 0:
        try:
            result = customersDF[customersDF['total spent']>0]['contact'].count()
        except Exception as ex:
            return result

    return result

def getMonthlyRevenueDF(ordersDF):
    result_df = pd.DataFrame()
    if ordersDF is not None or ordersDF.shape[0] != 0:
        try:
            ordersDF['date'] = pd.to_datetime(ordersDF['date'])
            result_df = ordersDF.groupby(ordersDF['date'].dt.strftime('%Y-%m'))['amount'].sum().sort_values()
            result_df = pd.concat([result_df], ignore_index=True, axis=1, sort=True)
            result_df.columns = ['amount']
        except Exception as ex:
            return pd.DataFrame()

    return result_df

def getFirstOrdersDate(ordersDF):
    result = ''
    if ordersDF is not None or ordersDF.shape[0] != 0:
        try:
            ordersDF['date'] = pd.to_datetime(ordersDF['date'])
            result = ordersDF.iloc[0]['date'].strftime("%Y-%m-%d")
        except Exception as ex:
            return ''

    return result


def getMonthSinceFirstOrder(firstOrderDate):
    if firstOrderDate is not None or firstOrderDate != "":
        current_date = datetime.now().strftime('%Y-%m-%d')
        return monthsdelta(firstOrderDate, current_date)
    return 0

def getRevVol(df):
    try:
        if df.loc[:, "amount"].mean() != 0 or df.loc[:, "amount"].mean() is not None:
            return df.loc[:, "amount"].std() / df.loc[:, "amount"].mean()
    except Exception as ex:
        return 0
    return 0

def generateDataPoints(inputMonthlyRevenueDF, numberOfCustomers, ordersDF):
    if inputMonthlyRevenueDF is None:
        return pd.DataFrame()
    if inputMonthlyRevenueDF.shape[0] == 0:
        return pd.DataFrame()
    datapointDF = pd.DataFrame()
    dataPointsDict = OrderedDict()
    firstOrderDate = getFirstOrdersDate(ordersDF)
    dataPointsDict['monthly_ecommerce_sales'] = round(inputMonthlyRevenueDF.loc[:, "amount"].mean(), 4)
    dataPointsDict['annual_ecommerce_sales'] = float(round(inputMonthlyRevenueDF.loc[:, "amount"].mean(), 4))*12
    dataPointsDict['monthly_avg_revenue'] = round(inputMonthlyRevenueDF.loc[:,"amount"].mean())
    dataPointsDict['revenue_volatility'] = getRevVol(inputMonthlyRevenueDF)
    dataPointsDict['number_of_customer_with_spent'] = numberOfCustomers
    dataPointsDict['month_since_first_order'] = getMonthSinceFirstOrder(firstOrderDate)

    for ind in dataPointsDict:
        datapointDF = pd.concat([datapointDF, pd.DataFrame([ind , dataPointsDict[ind]],index = ['data_point', 'value']).T], sort = True)
    datapointDF = datapointDF.set_index('data_point')
    return datapointDF



