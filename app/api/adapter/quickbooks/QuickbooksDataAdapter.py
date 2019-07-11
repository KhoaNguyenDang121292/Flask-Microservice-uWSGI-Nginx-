import jsonpath_rw_ext as jp
import pandas as pd
import numpy as np
from datetime import datetime
from collections import OrderedDict
from pandas.io.json import json_normalize
import re

from api.enums.quickbooks.AdapterLogs import QuickbooksAdapterLogs

from api.util.logging import Logging as logger


def convertNaNToFloat(value):
    value = value.strip()
    return float(value) if value else 0


def mapPnLToDF(requester, business_uuid, data):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(mapPnLToDF.__name__))
    result_df = pd.DataFrame()
    if data is not None or len(data) != 0:
        logger.info(requester, QuickbooksAdapterLogs.LOGS_PROFIT_AND_LOSS_AVAILABLE.value.format(business_uuid))
        for i in range(len(data)):
            distinct_columns = set()
            obj_row = {}

            distinct_columns.add('issued_date')
            obj_row['issued_date'] = data[i]['issued_at']

            dataRows = data[i]['data']
            dataRowDF = json_normalize(dataRows)
            for j in range(len(dataRowDF)):
                dataRows_Row = json_normalize(dataRowDF['Rows.Row'][j])
                try:
                    Rows_Row = dataRows_Row['Rows.Row']

                    for k in range(len(Rows_Row)):
                        Summary_Title = dataRows_Row['Summary.ColData'][k][0]['value']
                        Summary_Value = dataRows_Row['Summary.ColData'][k][1]['value']

                        if isinstance(Rows_Row[k], list):
                            Row_ColData = json_normalize(Rows_Row[k])['ColData']

                            for l in range(len(Row_ColData)):
                                ColData = json_normalize(Row_ColData[l])['value']
                                obj_row[ColData[0]] = float(convertNaNToFloat(ColData[1]))
                                distinct_columns.add(ColData[0])

                        distinct_columns.add(Summary_Title)
                        obj_row[Summary_Title] = float(convertNaNToFloat(Summary_Value))

                except Exception as ex:
                    continue

            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                         ignore_index=True, sort=False)
    else:
        logger.warning(requester, QuickbooksAdapterLogs.LOGS_PROFIT_AND_LOSS_UNAVAILABLE.value.format(business_uuid))
        return result_df

    return result_df.fillna(0)


def mapInvoicesToDF(requester, data):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(mapInvoicesToDF.__name__))
    if len(data) == 0:
        print('No json returned from service')
        return pd.DataFrame()
    else:
        invoicesDF = pd.DataFrame()
        for i in range(0,len(data)):
            rowList = []
            row = OrderedDict()
            if len(jp.match1('$.data.['+ str(i) +'].data.LinkedTxn', data)) != 0:
                row['type'] = jp.match1('$.data.['+ str(i) +'].data.LinkedTxn', data)[0]['TxnType']
            row['contact'] = jp.match1('$.data.['+ str(i) +'].contact', data)
            row['total_amount'] = jp.match1('$.data.['+ str(i) +'].data.TotalAmt', data)
            row['currency'] = jp.match1('$.data.['+ str(i) +'].currency', data)
            row['amount_unpaid'] = jp.match1('$.data.['+ str(i) +'].data.Balance', data)
            row['issued_date'] = datetime.strptime(jp.match1('$.data.['+ str(i) +'].data.TxnDate', data),'%Y-%m-%d')
            row['created_date'] = datetime.strptime(jp.match1('$.data.['+ str(i) +'].data.MetaData.CreateTime', data)[0:10],'%Y-%m-%d')
            row['due_date'] = datetime.strptime(jp.match1('$.data.['+ str(i) +'].data.DueDate', data),'%Y-%m-%d')
    
            time_today = datetime.now()
            row['term'] = (row['due_date'] - row['issued_date']).days
            row['created_till_data'] = (time_today - row['created_date']).days
            row['aging'] = (time_today - row['due_date']).days
            row['amount_paid'] = row['total_amount']- row['amount_unpaid']
    
            rowList.append(row)
            invoicesDF = invoicesDF.append(rowList, ignore_index = True, sort = True)
            
        COLUMN_NAMES = ['contact', 'currency', 'issued_date', 'due_date', 'aging', 'type', 'term', 'total_amount','amount_paid','amount_unpaid']
        invoicesDF = invoicesDF[COLUMN_NAMES]
        return invoicesDF.fillna(0)
    

def mapTransactionListToDF(requester, data):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(mapTransactionListToDF.__name__))
    if len(data) == 0:
        print('No biz data is actually returned')
        return pd.DataFrame()
    else:
        txnDF = pd.DataFrame()
        for i in range(0,len(data)):
            #if jp.match1('$.['+str(i)+'].endpoint', data) == 'transaction-lists':
            if len(data[i]['data']['Rows']) != 0:
                for ind in range(0,len(data[i]['data']['Rows']['Row'])):
                    rowList = []
                    row = OrderedDict()
                    row['date'] = datetime.strptime(jp.match1('$.data.['+str(i)+'].code', data)[13:23], '%d/%m/%Y')
                    row['currency'] = jp.match1('$.data.['+str(i)+'].currency', data) #[-4:30]
                    row['transaction_date'] = jp.match1('$.data.['+str(i)+'].data.Rows.Row.['+str(ind)+'].ColData.['+str(0)+'].value', data)
                    row['transaction_type'] = jp.match1('$.data.['+str(i)+'].data.Rows.Row.['+str(ind)+'].ColData.['+str(1)+'].value', data)
                    row['id'] = jp.match1('$.data.['+str(i)+'].data.Rows.Row.['+str(ind)+'].ColData.['+str(2)+'].value', data)
                    row['posting'] = jp.match1('$.data.['+str(i)+'].data.Rows.Row.['+str(ind)+'].ColData.['+str(3)+'].value', data)
                    row['Customer'] = jp.match1('$.data.['+str(i)+'].data.Rows.Row.['+str(ind)+'].ColData.['+str(4)+'].value', data)
                    row['Memo'] = jp.match1('$.data.['+str(i)+'].data.Rows.Row.['+str(ind)+'].ColData.['+str(5)+'].value', data)
                    row['Account Name'] = jp.match1('$.data.['+str(i)+'].data.Rows.Row.['+str(ind)+'].ColData.['+str(6)+'].value', data)
                    row['Other Accounts'] = jp.match1('$.data.['+str(i)+'].data.Rows.Row.['+str(ind)+'].ColData.['+str(7)+'].value', data)
                    row['amount'] = jp.match1('$.data.['+str(i)+'].data.Rows.Row.['+str(ind)+'].ColData.['+str(8)+'].value', data)
                    rowList.append(row)
                    txnDF = txnDF.append(rowList, ignore_index=True, sort = False)
        return txnDF.fillna(0)

def mapBalanceSheetToDF(requester, data):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(mapBalanceSheetToDF.__name__))
    if len(data) == 0:
        print('No biz data is actually returned')
        return pd.DataFrame()
    else:
        BalanceSheetDF = pd.DataFrame()
        for i in range(0, len(data)):
            rowList = []
            row = OrderedDict()
            metaDataNode = data[i]
            row['issued_date'] = metaDataNode['issued_at']
            rowsNode = data[i]['data']['Rows']['Row']
            for j in range(0, len(rowsNode)):
                if len(rowsNode) == 2:
                    dataNode_value = rowsNode[j]['Summary']['ColData']                 
                if len(dataNode_value) == 2:
                    row[dataNode_value[0]['value']] = dataNode_value[1]['value']
                if len(data[i]['data']['Rows']['Row']) == 2:             
                    asset = data[i]['data']['Rows']['Row'][0]['Rows']['Row'] # Asset Node
                    liability_and_equity = data[i]['data']['Rows']['Row'][1]['Rows']['Row'] # Liability and Equity Node
                for k in range(0, len(asset)):
                    asset_value = asset[k]['Summary']['ColData']
                    if len(asset_value) == 2:
                        row[asset_value[0]['value']] = asset_value[1]['value']
                for l in range(0, len(liability_and_equity)):
                    liability_and_equity_value = liability_and_equity[l]['Summary']['ColData']
                    if len(liability_and_equity_value) == 2:
                        row[liability_and_equity_value[0]['value']] = liability_and_equity_value[1]['value']
            rowList.append(row)   
            BalanceSheetDF = BalanceSheetDF.append(rowList, ignore_index = True, sort = True)
        return BalanceSheetDF


def getAgingList(requester, df):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(getAgingList.__name__))
    agingList = pd.DataFrame()

    if df is None:
        return agingList
    if df.shape[0] == 0:
        return agingList
    try:
        agingList['contact'] = df['contact']
        agingList['amount_unpaid'] = df.amount_unpaid

        nameList = ['1<>30', '31<>60', '61<>90', '91<>120', '121<>180', '181<>360', '>360']
        timeList = [0, 31, 61, 91, 121, 181, 361]
        for ind in range(0, len(nameList)):
            ind1 = np.array([1, 1, 1, 1, 1, 1, 1])
            due_amount = [df['amount_unpaid'].values[i] if pd.Series(
                ind1 * (df.aging.values[i] >= timeList)).sum() == ind + 1 else 0.0 for i in range(0, df.shape[0])]
            agingList[nameList[ind]] = due_amount
        agingList = agingList.groupby(['contact']).sum().sort_values(by='amount_unpaid', ascending=False)
        agingList['amount_unpaid_ratio'] = agingList['amount_unpaid'] / agingList['amount_unpaid'].sum()
    except Exception as ex:
        logger.warning(requester, QuickbooksAdapterLogs.LOGS_AGING_LIST_WARNING.value.format(str(ex)))
        return agingList

    return agingList

def get_debit_trx_list_from_trx_list(requester, inputTrxListDF):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(getAgingList.__name__))
    if inputTrxListDF is None:
        return pd.DataFrame()
    if inputTrxListDF.shape[0] == 0:
        return pd.DataFrame()
    trxType = ['Bill', 'Expense','Payment']
    return inputTrxListDF[inputTrxListDF['transaction_type'].isin(trxType)]


def getTopClientList(requester, df):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(getTopClientList.__name__))
    if df is None:
        return pd.DataFrame()
    if df.shape[0] == 0:
        return pd.DataFrame()

    try:
        clients_cctn = pd.DataFrame(df.groupby('contact')['amount_paid'].sum())
        clients_cctn = clients_cctn[clients_cctn['amount_paid'] != 0]
        clients_cctn['% as of total'] = clients_cctn['amount_paid'] / clients_cctn['amount_paid'].sum()
        clients_cctn = clients_cctn.sort_values(by='amount_paid', ascending=False)
    except Exception as ex:
        logger.warning(requester, QuickbooksAdapterLogs.LOGS_TOP_CLIENT_WARNING.value.format(str(ex)))
        return pd.DataFrame()

    return clients_cctn


def getUnpaidTopClientList(requester, df):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(getUnpaidTopClientList.__name__))
    if df is None:
        return pd.DataFrame()
    if df.shape[0] == 0:
        return pd.DataFrame()
    try:
        clients_cctn = pd.DataFrame(df.groupby('contact')['amount_unpaid'].sum())
        clients_cctn = clients_cctn[clients_cctn['amount_unpaid'] != 0]
        clients_cctn['% as of total'] = clients_cctn['amount_unpaid'] / clients_cctn['amount_unpaid'].sum()
        clients_cctn = clients_cctn.sort_values(by='amount_unpaid', ascending=False)
    except Exception as ex:
        logger.warning(requester, QuickbooksAdapterLogs.LOGS_UNPAID_TOP_CLIENT_WARNING.value.format(str(ex)))
        return pd.DataFrame()

    return clients_cctn


def get_pnl_cogs_items(requester, data):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(get_pnl_cogs_items.__name__))
    pnlDF = pd.DataFrame()
    for ind in range(0,len(data)):
        amount = pd.Series([float(re.sub(',','.',re.sub(r'[ .]','',ind))) for ind in jp.match('$.['+ str(ind) +'].data.cost_of_good_sold.accounts[*].array[*].data[*].balance', data)])
        item = pd.Series(jp.match('$.['+ str(ind) +'].data.cost_of_good_sold.accounts[*].array[*].name', data))
        rowList = pd.DataFrame(amount).T
        rowList.columns = item
        pnlDF = pd.concat([pnlDF, rowList], sort = True)
    
    pnlDF.index = pd.date_range(start = jp.match1('$.['+ str(len(data)-1) + '].[issued_at].[date]',data)[0:10], end = jp.match1('$.[0].[issued_at].[date]',data)[0:10], freq = 'MS')[-1::-1]
    
    return pnlDF.fillna(0).T

def get_expense_items(requester, data):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(get_expense_items.__name__))
    pnlDF = pd.DataFrame()
    for ind in range(0,len(data)):
        amount = pd.Series([float(re.sub(',','.',re.sub(r'[ .]','',ind))) for ind in jp.match('$.['+ str(ind) +'].data.expense.accounts[*].array[*].data[*].balance', data)])
        #print(amount)
        item = pd.Series(jp.match('$.['+ str(ind) +'].data.expense.accounts[*].array[*].name', data))
        #print(item)
        rowList = pd.DataFrame(amount).T
        rowList.columns = item
        pnlDF = pd.concat([pnlDF, rowList], sort = True)
    
    pnlDF.index = pd.date_range(start = jp.match1('$.['+ str(len(data)-1) + '].[issued_at].[date]',data)[0:10], end = jp.match1('$.[0].[issued_at].[date]',data)[0:10], freq = 'MS')[-1::-1]
    
    return pnlDF.fillna(0).T.drop('General & Administrative Expenses')

def convertIndoCurrencyToNumber(ind):
    return float(re.sub(',','.',re.sub(r'[ .]','',ind)))

def computeIncreasingMultiplier(xVal, beginXV,endingXV,linetype):
    y = 0
    if linetype == 'up':
        slope = 1/(endingXV - beginXV)
    else:
        slope = -1/(endingXV - beginXV)
    incp = -slope*beginXV
    y = xVal*slope + incp
    if y <= 0:
        y = 0
    elif y >= 1:
        y = 1
    return y

def getMonthlyAvgRevenue(pnlDF):
    result = 0
    try:
        if pnlDF.empty is False or pnlDF.shape[0] != 0 or pnlDF is not None:
            result = round(pnlDF.loc[:,"Total Revenue"].mean())
    except Exception as ex:
        print("WARNING - " + str(ex))
        return 0

    return result


def generateDataPoints(requester, pnlDF, salesInvoicesDF, debitTrxListDF):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(generateDataPoints.__name__))
    datapointDF = pd.DataFrame()
    dataPointsDict = OrderedDict()
    try:
        dataPointsDict['monthly_avg_revenue'] = getMonthlyAvgRevenue(pnlDF)
        dataPointsDict['deduction'] = 'To enter the irrelevant amount'
        dataPointsDict['revenue_volatility'] = float(getRevVol(requester, pnlDF))
        dataPointsDict['profit_margin'] = float(getProfitMargin(requester, pnlDF))
        dataPointsDict['net_debit_transaction_per_month'] = float(getAvgMonthlyReceivableInvoicesCount(requester,
                                                                                                       salesInvoicesDF))
        dataPointsDict['net_credit_transaction_per_month'] = float(getAvgMonthlyPayableInvoicesCount(requester,
                                                                                                     debitTrxListDF))
    except Exception as ex:
        logger.warning(requester, QuickbooksAdapterLogs.LOGS_GENERATE_DATA_POINTS_WARNING.value.format(str(ex)))
        return pd.DataFrame()

    for ind in dataPointsDict:
        datapointDF = pd.concat([datapointDF, pd.DataFrame([ind , dataPointsDict[ind]],index = ['data_point', 'value']).T], sort = True)

    datapointDF = datapointDF.set_index('data_point')
    datapointDF = datapointDF.fillna(0)
    return datapointDF

def getAvgMonthlyPayableInvoicesCount(requester, df):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(getAvgMonthlyPayableInvoicesCount.__name__))
    result = 0
    if df is not None or df.shape[0] != 0:
        try:
            result = df.groupby(df['date'].dt.strftime('%B'))['amount'].count().sort_values()
            result = result.to_frame()
            if result.shape[0] == 0:
                return 0
            result = result['amount'].mean()
            return result
        except Exception as ex:
            logger.warning(requester, QuickbooksAdapterLogs.LOGS_GET_MONTHLY_PAYABLES_INVOICES_WARNING.value.format(str(ex)))
            return 0
    return result


def getAvgMonthlyReceivableInvoicesCount(requester, df):
    logger.info(requester,
                QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(getAvgMonthlyPayableInvoicesCount.__name__))

    result = 0
    if df is None or df.shape[0] == 0:
        return 0
    try:
        result = df.groupby(df['issued_date'].dt.strftime('%B'))['total_amount'].count().sort_values()
        return result.to_frame()['total_amount'].mean()
    except Exception as ex:
        logger.warning(requester, QuickbooksAdapterLogs.LOGS_GET_MONTHLY_RECEIVABLE_INVOICES_WARNING.value.format(str(ex)))
        return result

    return result


def getRevVol(requester, pnlDF):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(getRevVol.__name__))
    try:
        if pnlDF is not None or pnlDF.shape[0] != 0 or \
                pnlDF.loc[:, "amount"].mean() != 0 or pnlDF.loc[:, "amount"].mean() is not None:
            return pnlDF.loc[:,"Total Revenue"].std()/pnlDF.loc[:,"Total Revenue"].mean()
    except Exception as ex:
        logger.warning(requester, QuickbooksAdapterLogs.LOGS_GET_REV_WARNING.value.format(str(ex)))
        return 0
    return 0


def getProfitMargin(requester, pnlDF):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(getProfitMargin.__name__))
    try:
        if pnlDF['Total Revenue'].sum() != 0 or pnlDF['Total Revenue'].sum() is not None:
            return pnlDF['Profit/Loss'].sum()/pnlDF['Total Revenue'].sum()
    except Exception as ex:
        logger.warning(requester, QuickbooksAdapterLogs.LOGS_GET_REV_WARNING.value.format(str(ex)))
        return 0
    return 0


def mapTransactionToDF(requester, business_uuid, data):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(mapTransactionToDF.__name__))
    result_df = pd.DataFrame()

    if data is not None or len(data) != 0:
        for i in range(len(data)):
            distinct_columns = set()
            obj_row = {}

            distinct_columns.add('date')
            distinct_columns.add('currency')
            distinct_columns.add('transaction_date')
            distinct_columns.add('transaction_type')
            distinct_columns.add('id')
            distinct_columns.add('posting')
            distinct_columns.add('Customer')
            distinct_columns.add('Memo')
            distinct_columns.add('Account Name')
            distinct_columns.add('Other Accounts')
            distinct_columns.add('amount')

            obj_row['date'] = data[i]['issued_at']
            obj_row['currency'] = data[i]['currency']

            dataRows = data[i]['data']
            dataRowDF = json_normalize(dataRows)
            for j in range(len(dataRowDF)):
                try:
                    dataRows_Row = dataRowDF['Rows.Row'][j]

                    for k in range(len(dataRows_Row)):
                        Row_ColData = dataRows_Row[k]['ColData']

                        obj_row['transaction_date'] = Row_ColData[0]['value']
                        obj_row['transaction_type'] = Row_ColData[1]['value']
                        obj_row['id'] = Row_ColData[2]['value']
                        obj_row['posting'] = Row_ColData[3]['value']
                        obj_row['Customer'] = Row_ColData[4]['value']
                        obj_row['Memo'] = Row_ColData[5]['value']
                        obj_row['Account Name'] = Row_ColData[6]['value']
                        obj_row['Other Accounts'] = Row_ColData[7]['value']
                        obj_row['amount'] = Row_ColData[8]['value']

                except Exception as ex:
                    continue

            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                         ignore_index=True, sort=False)

    else:
        logger.warning(requester, QuickbooksAdapterLogs.LOGS_TRANSACTION_UNAVAILABLE.value.format(business_uuid))

    return result_df.fillna(0)


def mapInvoicesToDF(requester, business_uuid, data):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(mapInvoicesToDF.__name__))
    result_df = pd.DataFrame()
    time_today = datetime.now()

    if data is not None or len(data) != 0:
        for i in range(len(data)):
            distinct_columns = set()
            obj_row = {}

            distinct_columns.add('contact')
            distinct_columns.add('currency')
            distinct_columns.add('issued_date')
            distinct_columns.add('due_date')
            distinct_columns.add('aging')
            distinct_columns.add('type')
            distinct_columns.add('term')
            distinct_columns.add('total_amount')
            distinct_columns.add('amount_paid')
            distinct_columns.add('amount_unpaid')

            obj_row['contact'] = data[i]['contact']
            obj_row['currency'] = data[i]['currency']
            obj_row['issued_date'] = datetime.strptime(data[i]['issued_at'], '%Y-%m-%d')

            data_data = json_normalize(data[i])

            obj_row['due_date'] = data_data['data.DueDate'][0]
            obj_row['aging'] = (time_today - datetime.strptime(data_data['data.DueDate'][0], '%Y-%m-%d')).days

            if len(data_data['data.LinkedTxn'][0]) > 0:
                obj_row['type'] = json_normalize(data_data['data.LinkedTxn'][0])['TxnType'][0]
            else:
                obj_row['type'] = 0

            obj_row['term'] = (datetime.strptime(data_data['data.DueDate'][0], '%Y-%m-%d') -
                               datetime.strptime(data_data['issued_at'][0], '%Y-%m-%d')).days
            obj_row['total_amount'] = data_data['data.TotalAmt'][0]
            obj_row['amount_paid'] = data_data['data.Balance'][0]
            obj_row['amount_unpaid'] = float(data_data['data.TotalAmt'][0]) - float(data_data['data.Balance'][0])

            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                         ignore_index=True, sort=False)
    else:
        logger.warning(requester, QuickbooksAdapterLogs.LOGS_INVOICES_UNAVAILABLE.value.format(business_uuid))

    return result_df.fillna(0)


def mapBalanceSheetToDF(requester, business_uuid, data):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(mapBalanceSheetToDF.__name__))
    result_df = pd.DataFrame()

    if data is not None or len(data) != 0:

        data = data
        for i in range(len(data)):
            distinct_columns = set()
            obj_row = {}

            distinct_columns.add('issued_date')
            obj_row['issued_date'] = data[i]['issued_at']

            rows_row1 = json_normalize(data[i]['data'])['Rows.Row']

            for j in range(len(rows_row1)):
                rows_row2 = json_normalize(rows_row1[j])['Rows.Row']
                for k in range(len(rows_row2)):
                    try:
                        rows_row3 = json_normalize(rows_row2[k])['Rows.Row']

                    except Exception as ex:
                        continue

                    for m in range(len(rows_row3)):
                        try:
                            row3_col_data = json_normalize(rows_row3[m])
                            if 'ColData' in row3_col_data.columns.values:
                                for p in range(len(row3_col_data['ColData'])):
                                    col_data = row3_col_data['ColData'][p]

                                    if isinstance(col_data, list):
                                        distinct_columns.add(col_data[0]['value'])
                                        obj_row[col_data[0]['value']] = col_data[1]['value']

                            if 'Rows.Row' in row3_col_data.columns.values:
                                b = row3_col_data['Rows.Row']
                                for u in b:
                                    if isinstance(u, list):
                                        col_data = json_normalize(u)['ColData']
                                        for o in range(len(col_data)):
                                            distinct_columns.add(col_data[o][0]['value'])
                                            obj_row[col_data[o][0]['value']] = col_data[o][1]['value']
                        except Exception as ex:
                            continue

            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                         ignore_index=True, sort=False)
    else:
        logger.warning(requester, QuickbooksAdapterLogs.LOGS_BALANCE_SHEET_UNAVAILABLE.value.format(business_uuid))

    return result_df.fillna(0)

def getDebitTrxListFromTrxList(requester, inputTrxListDF):
    logger.info(requester, QuickbooksAdapterLogs.LOGS_START_FUNCTION.value.format(getDebitTrxListFromTrxList.__name__))
    result_df = pd.DataFrame()
    if inputTrxListDF is not None or inputTrxListDF.shape[0] != 0:
        trxType = ['Bill', 'Expense','Payment']
        try:
            inputTrxListDF = inputTrxListDF[inputTrxListDF['transaction_type'].isin(trxType)]
            inputTrxListDF['date'] = inputTrxListDF['date'].astype('datetime64[ns]')

            result = inputTrxListDF
            return result
        except Exception as ex:
            logger.warning(requester, QuickbooksAdapterLogs.LOGS_DEBIT_TRX_WARNING.value.format(str(ex)))
            return pd.DataFrame()

    return result_df

