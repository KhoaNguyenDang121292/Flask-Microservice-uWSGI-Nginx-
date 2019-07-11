import jsonpath_rw_ext as jp
import pandas as pd
import numpy as np
from datetime import datetime
from collections import OrderedDict
from pandas.io.json import json_normalize

from api.enums.xero.AdapterLogs import XeroAdapterLogs

from api.util.logging import Logging as logger


def mapPnLDF(requester, business_uuid, data):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(mapPnLDF.__name__))
    if data is not None or len(data) != 0:
        result_df = pd.DataFrame()

        for i in range(len(data)):
            distinct_columns = set()
            obj_row = {}

            distinct_columns.add('issue_date')
            obj_row['issue_date'] = data[i]['issued_at']

            dataRow = json_normalize(data[i]['data'])['Rows']
            for element in dataRow:
                if isinstance(element, list):
                    dataRow_Cells = json_normalize(element)['Cells']
                    for k in range(len(dataRow_Cells)):
                        distinct_columns.add(json_normalize(dataRow_Cells[k])['Value'][0])
                        obj_row[json_normalize(dataRow_Cells[k])['Value'][0]] = float(json_normalize(dataRow_Cells[k])['Value'][1])

            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                                        ignore_index=True, sort=True)
    else:
        logger.warning(requester, XeroAdapterLogs.LOGS_PROFIT_AND_LOSS_UNAVAILABLE.value.format(business_uuid))

    return result_df.fillna(0)


def mapPnLDF_Old(data):
    if len(data) == 0:
        print('No biz data is actually returned')
        return pd.DataFrame()
    else:
        pnlDF = pd.DataFrame()
        nameMappingDict = {'Gross Profit':'gross_profit',
                     'Net Profit':'net_profit',
                     'Total Income':'total_revenue',
                     'Total Other Income':'total_other_income',
                     'Total Operating Expenses':'total_operating_expenses'}
        for item in range(0,len(data)):

            rowList = []
            row = OrderedDict()
            dateStr = data[item]['issued_at'][0:10] ### in previous json data, it is data[item]['issued_at'][date][0:10]
            row['issue_date'] = datetime.strptime(dateStr, '%Y-%m-%d')
            for i in range(1, len(data[item]['data'])):
                if 'Rows' in list(data[item]['data'][i].keys()):
                    #print(len(data[item]['data']))
                    lth = '$.data.['+str(item)+'].data['+str(i)+'].Rows'
                    fin = len(jp.match1(lth,data))-1
                    path = '$.data.['+str(item)+'].data['+str(i)+'].Rows['+str(fin)+'].Cells[0].Value'
                    itemHeader = jp.match1(path, data)
                    path = '$.data.['+str(item)+'].data['+str(i)+'].Rows['+str(fin)+'].Cells[1].Value'
                    itemValue = jp.match1(path, data)
                    if itemHeader in nameMappingDict.keys():
                        row[nameMappingDict[itemHeader]] = float(itemValue)
                    else:
                        print('\33[31m','ERROR::mapXeroPnLToDF','Name' + itemHeader +' has been changed, Please trace back to JSON data to reassure')
            #print(row)
            rowList.append(row)
            pnlDF = pnlDF.append(rowList, ignore_index=True, sort = True)

        pnlDF['profit_margin'] = pnlDF['net_profit']/pnlDF['total_revenue']

        pnlDF.where(pnlDF.notnull(), 0)
        pnlDF.fillna(0, inplace=True)
        #pnlDF.set_index('issue_date', inplace=True)
        COLUMN_NAMES = ['issue_date',  'total_revenue', 'gross_profit', 'total_operating_expenses', 'net_profit', 'profit_margin']
        pnl = pnlDF[COLUMN_NAMES]

        return pnl


def mapCOADF(requester, business_uuid, data):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(mapCOADF.__name__))
    result_df = pd.DataFrame()
    if data is not None or len(data) != 0:
        distinct_columns = set()
        data = data
    
        for i in range(len(data)):
            distinct_columns = distinct_columns|(set(list(json_normalize(data[i]['data']).columns.values)))
            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(data[i]['data'], columns=list(distinct_columns), index=[i]),
                                                    ignore_index=True, sort=True)
    else:
        logger.warning(requester, XeroAdapterLogs.LOGS_CHART_OF_ACCOUNT_UNAVAILABLE.value.format(business_uuid))

    return result_df.fillna(0)



def mapInvoicesDFNew(requester, business_uuid, data):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(mapInvoicesDFNew.__name__))
    if data is not None or len(data) != 0:
        result_df = pd.DataFrame()
        distinct_columns = set()
        data = json_normalize(data)

        time_today = datetime.now()
        distinct_columns = ['updated_at', 'contact', 'currency', 'issued_date', 'due_date', 'aging', 'type',
                           'term', 'total_amount', 'amount_paid', 'amount_unpaid']

        for i in range(len(data)):
            try:
                obj_row = {}

                obj_row['updated_at'] = data['data.UpdatedDateUTC'][i][0:10]
                obj_row['contact'] = data['contact'][i]
                obj_row['currency'] = data['currency'][i]
                obj_row['issued_date'] = data['issued_at'][i]
                obj_row['due_date'] = data['data.DueDate'][i]
                obj_row['aging'] = (time_today - datetime.strptime(data['data.DueDate'][i], '%Y-%m-%d')).days
                obj_row['type'] = data['type'][i]
                obj_row['term'] = (datetime.strptime(data['data.DueDate'][i], '%Y-%m-%d')
                                                    - datetime.strptime(data['issued_at'][i], '%Y-%m-%d')).days
                obj_row['total_amount'] = float(data['data.Total'][i])
                obj_row['amount_paid'] = float(data['data.AmountPaid'][i])
                obj_row['amount_unpaid'] = float(data['data.AmountDue'][i])

                # Combine into result DF
                result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                                            ignore_index=True)
            except Exception as ex:
                continue
    else:
        logger.warning(requester, XeroAdapterLogs.LOGS_INVOICES_UNAVAILABLE.value.format(business_uuid))

    return result_df.fillna(0)


def checkDueDate(field):
    if isinstance(field, float):
        return True
    return False


def getPurchaseInvoicesDF(requester, allInvoicesDF):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getPurchaseInvoicesDF.__name__))
    resulf_df = pd.DataFrame()
    if allInvoicesDF is None or allInvoicesDF.shape[0] == 0:
        return pd.DataFrame()
    try:
        resulf_df = allInvoicesDF[allInvoicesDF['type']=='ACCPAY']
        return resulf_df
    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_PURCHASE_INVOICES_ERROR.value.format(str(ex)))
        return pd.DataFrame()

    return resulf_df


def getSalesInvoicesDF(requester, allInvoicesDF):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getSalesInvoicesDF.__name__))
    resulf_df = pd.DataFrame()
    if allInvoicesDF is None or allInvoicesDF.shape[0] == 0:
        return pd.DataFrame()
    try:
        resulf_df = allInvoicesDF[allInvoicesDF['type']=='ACCREC']
        return resulf_df
    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_PURCHASE_INVOICES_ERROR.value.format(str(ex)))
        return pd.DataFrame()

    return resulf_df


def mapBankTransactionDF(requester, data):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getSalesInvoicesDF.__name__))
    if len(data) == 0:
       print('No biz data is actually returned')
       return pd.DataFrame()
    else:
        bxDF = pd.DataFrame()
        for i in range(0,len(data)):
            rowList = []
            rows = OrderedDict()

            rows['date'] = datetime.strptime(jp.match1('$.['+ str(i) +'].data.Date', data),'%Y-%m-%d')
            rows['type'] = jp.match1('$.['+ str(i) +'].data.Type', data)
            rows['status'] = jp.match1('$.['+ str(i) +'].data.Status', data)
            rows['total'] = jp.match1('$.['+ str(i) +'].data.Total', data)
            rows['tax'] = jp.match1('$.['+ str(i) +'].data.TotalTax', data)
            rows['subtotal'] = jp.match1('$.['+ str(i) +'].data.SubTotal', data)
            rows['contact'] = jp.match1('$.['+ str(i) +'].data.Contact.Name', data)
            rows['currency'] = jp.match1('$.['+ str(i) +'].data.CurrencyCode', data)
            rows['account'] = jp.match1('$.['+ str(i) +'].data.BankAccount.AccountID', data)
            rows['other_info'] = jp.match1('$.['+ str(i) +'].data.LineItems.['+ str(0) +'].Description', data)

            rowList.append(rows)
            bxDF = bxDF.append(rowList, ignore_index = True, sort = True)
        return bxDF


def getAgingList(requester, df):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getAgingList.__name__))
    agingList = pd.DataFrame()
    if(df is None or df.shape[0]==0):
        return pd.DataFrame()
    try:
        agingList['contact'] = df.contact
        agingList['amount_unpaid'] = df.amount_unpaid
        
        nameList = ['1<>30', '31<>60', '61<>90', '91<>120', '121<>180', '181<>360', '>360']
        timeList = [0, 31, 61, 91, 121, 181, 361]
        for ind in range(0,len(nameList)):
            ind1 = np.array([1,1,1,1,1,1,1])
            due_amount = [df['amount_unpaid'].values[i] if pd.Series(ind1*(df.aging.values[i]>=timeList)).sum() == ind+1 else 0.0 for i in range(0, df.shape[0])]
            agingList[nameList[ind]] = due_amount
        agingList = agingList.groupby(['contact']).sum().sort_values(by = 'amount_unpaid', ascending = False)
        agingList['amount_unpaid_ratio'] = agingList['amount_unpaid']/agingList['amount_unpaid'].sum()
    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_GET_AGING_LIST_ERROR.value.format(str(ex)))
        return pd.DataFrame()

    return agingList


def getTopClientList(requester, df):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getTopClientList.__name__))
    if df is None or df.shape[0] == 0:
        return pd.DataFrame()
    result_df = pd.DataFrame()
    try:
        result_df = pd.DataFrame(df.groupby('contact')['amount_paid'].sum())
        result_df = result_df[result_df['amount_paid'] != 0]
        result_df['% as of total'] = result_df['amount_paid'] / result_df['amount_paid'].sum()
        result_df = result_df.sort_values(by='amount_paid', ascending=False)
    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_GET_TOP_CLIENT_ERROR.value.format(str(ex)))
        return pd.DataFrame()

    return result_df


def getUnpaidTopClientList(requester, df):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getUnpaidTopClientList.__name__))
    result_df = pd.DataFrame()
    if df is None or df.shape[0] == 0:
        return pd.DataFrame()
    try:
        result_df = pd.DataFrame(df.groupby('contact')['amount_unpaid'].sum())
        result_df = result_df[result_df['amount_unpaid'] != 0]
        result_df['% as of total'] = result_df['amount_unpaid'] / result_df['amount_unpaid'].sum()
        result_df = result_df.sort_values(by='amount_unpaid', ascending=False)
    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_UNPAID_TOP_CLIENT_ERROR.value.format(str(ex)))
        return pd.DataFrame()

    return result_df


def mapPaymentDF(data):
    if len(data) == 0:
       print('No biz data is actually returned')
       return pd.DataFrame()
    else:
        pmtDF = pd.DataFrame()
        for i in range(0, len(data)):
            rowList = []
            rows = OrderedDict()
            rows['accountID'] = jp.match1('$.['+ str(i) +'].data.Account.AccountID',data)
            rows['invoiceID'] =  jp.match1('$.['+ str(i) +'].data.Invoice.InvoiceID',data)
            rows['contact'] = jp.match1('$.['+ str(i) +'].data.Invoice.Contact.Name',data)
            rows['type'] = jp.match1('$.['+ str(i) +'].data.Invoice.Type',data)
            rows['currency'] = jp.match1('$.['+ str(i) +'].data.Invoice.CurrencyCode',data)
            rows['currency_rate'] = jp.match1('$.['+ str(i) +'].data.CurrencyRate',data)       
            rows['status'] = jp.match1('$.['+ str(i) +'].data.Status',data)
            rows['date'] = datetime.strptime(jp.match1('$.['+ str(i) +'].data.Date',data),'%Y-%m-%d')
            rows['total'] =float(jp.match1('$.['+ str(i) +'].data.Amount',data))
            rowList.append(rows)
            pmtDF = pmtDF.append(rowList, ignore_index = True, sort = True)
        return pmtDF


def mapBalanceSheetDF(requester, business_uuid, data):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(mapBalanceSheetDF.__name__))
    result_df = pd.DataFrame()

    try:
        if data is not None or len(data) != 0:
            for i in range(len(data)):
                distinct_columns = set()
                obj_row = {}

                distinct_columns.add('issue_date')
                obj_row['issue_date'] = data[i]['issued_at']

                dataRow = json_normalize(data[i]['data'])['Rows']
                for element in dataRow:
                    if isinstance(element, list):
                        dataRow_Cells = json_normalize(element)['Cells']
                        for k in range(len(dataRow_Cells)):
                            distinct_columns.add(json_normalize(dataRow_Cells[k])['Value'][0])
                            obj_row[json_normalize(dataRow_Cells[k])['Value'][0]] = float(
                                json_normalize(dataRow_Cells[k])['Value'][1])

                # Combine into result DF
                result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                             ignore_index=True, sort=True)
        else:
            logger.warning(requester, XeroAdapterLogs.LOGS_MAP_BALANCE_SHEET_UNAVAILABLE.value.format(business_uuid))

    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_MAP_BALANCE_SHEET_ERROR.value.format(str(ex)))

    return result_df.fillna(0)


def mapPnLToDF_total(data):
    pnlDF_total = pd.DataFrame()
    for item in range(0,len(data)):
        #rowList = OrderedDict()
        rowtype_detect = jp.match('$.data.['+str(item)+'].data[*].Rows[*].Cells[*].Value',data)
        item_list = rowtype_detect[::2]
        value_list = rowtype_detect[1::2]
        rowList = pd.DataFrame(value_list).T
        rowList.columns = item_list
        pnlDF_total = pd.concat([pnlDF_total, rowList], sort = True)
    return pnlDF_total.fillna(0)#pnlDF_total


def get_top_client_list(df):
    if df is None:
        return pd.DataFrame()
    if df.shape[0] == 0:
        return pd.DataFrame()
    clients_cctn = pd.DataFrame(df.groupby('contact')['amount_paid'].sum())
    clients_cctn = clients_cctn[clients_cctn['amount_paid'] != 0]
    clients_cctn['% as of total'] = clients_cctn['amount_paid'] / clients_cctn['amount_paid'].sum()
    clients_cctn = clients_cctn.sort_values(by='amount_paid', ascending=False)
    return clients_cctn


def get_unpaid_top_client_list(df):
    clients_cctn = pd.DataFrame(df.groupby('contact')['amount_unpaid'].sum())
    clients_cctn = clients_cctn[clients_cctn['amount_unpaid'] != 0]
    clients_cctn['% as of total'] = clients_cctn['amount_unpaid'] / clients_cctn['amount_unpaid'].sum()
    clients_cctn = clients_cctn.sort_values(by='amount_unpaid', ascending=False)
    return clients_cctn


def classified_inv_df(dafrm):
    acc_term = input('Please input ACCREC (for receivable) or ACCPAY (for payable): ')
    da1 = dafrm[(dafrm['Type'] == acc_term) & (dafrm['status'] != 'PAID')]
    return da1


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


def getAvgMonthlyReceivableInvoicesCount(requester, inputDF):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getAvgMonthlyReceivableInvoicesCount.__name__))
    result = 0
    try:
        result = inputDF.groupby(inputDF['issued_date'])['total_amount'].count().sort_values()
        return result.to_frame()['total_amount'].mean()
    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_MONTHLY_RECEIVABLE_INVOICES_ERROR.value.format(str(ex)))
        return 0
    return result


def getAvgMonthlyPayableInvoicesCount(requester, inputDF):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getAvgMonthlyPayableInvoicesCount.__name__))
    result = 0
    try:
        result = inputDF.groupby(inputDF['issued_date'])['total_amount'].count().sort_values()
        return result.to_frame()['total_amount'].mean()
    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_MONTHLY_PAYABLE_INVOICES_ERROR.value.format(str(ex)))
        return 0
    return result


def getAvgMonthlyEndingBalance(requester, inputDF):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getAvgMonthlyPayableInvoicesCount.__name__))
    result = 0
    try:
        return inputDF['Total Bank'].mean()
    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_MONTHLY_PAYABLE_INVOICES_ERROR.value.format(str(ex)))
        return 0
    return result


def getRevVol(requester, pnlDF, coaDF):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getRevVol.__name__))
    result = 0
    try:
        monthly_rev_list = []
        revenue_acct_list = coaDF[coaDF.Class=='REVENUE']['Name'].tolist()
        for index,row in pnlDF.iterrows():
            monthly_rev = 0
            for acct in revenue_acct_list:
                if acct in pnlDF:
                    monthly_rev += row[acct]
            monthly_rev_list.append(monthly_rev)
        result = np.std(monthly_rev_list) / np.mean(monthly_rev_list)
    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_REV_VOL_ERROR.value.format(str(ex)))
        return 0
    return result

def getMonthlyAvgRev(requester, pnlDF, coaDF):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getMonthlyAvgRev.__name__))
    total_rev = 0
    try:
        for item in coaDF[coaDF.Class=='REVENUE']['Name'].tolist():
            if item in pnlDF:
                total_rev += pnlDF[item].mean()
    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_MONTHLY_AVG_REV_ERROR.value.format(str(ex)))
        return 0

    return total_rev


def getMonthlyAvgExp(requester, pnlDF, coaDF):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getMonthlyAvgRev.__name__))
    total_exp = 0
    try:
        for item in coaDF[coaDF.Class == 'EXPENSE']['Name'].tolist():
            if item in pnlDF:
                total_exp += pnlDF[item].mean()

    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_MONTHLY_AVG_EXP_ERROR.value.format(str(ex)))
        return 0

    return total_exp


def calculateProfitMargin(requester, monthly_avg_revenue, monthly_avg_expense):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(calculateProfitMargin.__name__))
    if monthly_avg_revenue == 0:
        return 0
    return (monthly_avg_revenue-monthly_avg_expense)/monthly_avg_revenue


def getDataPointsDF(requester, business_uuid, pnlDF, salesInvoicesDF, payableInvoicesDF, balanceSheetDF, coaDF):
    logger.info(requester, XeroAdapterLogs.LOGS_START_FUNCTION.value.format(getDataPointsDF.__name__))
    datapointDF = pd.DataFrame()                
    dataPointsDict = OrderedDict()

    try:
        if pnlDF is not None and pnlDF.shape[0]>0:
            monthly_avg_revenue = getMonthlyAvgRev(requester, pnlDF, coaDF)
            monthly_avg_expense = getMonthlyAvgExp(requester, pnlDF, coaDF)
            dataPointsDict['monthly_avg_revenue'] = monthly_avg_revenue
            dataPointsDict['monthly_avg_expense'] = monthly_avg_expense
            dataPointsDict['revenue_volatility'] = getRevVol(requester, pnlDF, coaDF)
            dataPointsDict['profit_margin'] = calculateProfitMargin(requester, monthly_avg_revenue, monthly_avg_expense)

        if salesInvoicesDF is not None and salesInvoicesDF.shape[0] > 0:
            dataPointsDict['net_debit_transaction_per_month'] = \
                getAvgMonthlyReceivableInvoicesCount(requester, salesInvoicesDF)
            dataPointsDict['net_debit_transaction_per_month'] = \
                getAvgMonthlyReceivableInvoicesCount(requester, salesInvoicesDF)

        if payableInvoicesDF is not None and payableInvoicesDF.shape[0] > 0:
            dataPointsDict['net_credit_transaction_per_month'] = \
                getAvgMonthlyPayableInvoicesCount(requester, payableInvoicesDF)

        if balanceSheetDF is not None and balanceSheetDF.shape[0] > 0:
            dataPointsDict['average_monthly_ending_balance'] = \
                getAvgMonthlyEndingBalance(requester, balanceSheetDF)

        for ind in dataPointsDict:
            datapointDF = pd.concat([datapointDF, pd.DataFrame([ind ,
                dataPointsDict[ind]], index=['data_point', 'value']).T], sort=True)

        datapointDF = datapointDF.set_index('data_point')
        datapointDF = datapointDF.fillna(0)
    except Exception as ex:
        logger.warning(requester, XeroAdapterLogs.LOGS_DATA_POINTS_ERROR.value.format(business_uuid, str(ex)))
        return datapointDF


    return datapointDF


def mapInvoicesDF_New(business_uuid, data):
    result_df = pd.DataFrame()
    if data is not None or len(data) != 0:
        data = json_normalize(data)

        time_today = datetime.now()
        distinct_columns = ['updated_at', 'contact', 'currency', 'issued_date', 'due_date', 'aging', 'type',
                           'term', 'total_amount', 'amount_paid', 'amount_unpaid']

        for i in range(len(data)):
            obj_row = {}

            obj_row['updated_at'] = data['data.UpdatedDateUTC'][i][0:10]
            obj_row['contact'] = data['contact'][i]
            obj_row['currency'] = data['currency'][i]
            obj_row['issued_date'] = data['issued_at'][i]
            obj_row['due_date'] = data['data.DueDate'][i]

            if isinstance(data['data.DueDate'][i], float):
                obj_row['aging'] = 0
            else:
                obj_row['aging'] = (time_today - datetime.strptime(data['data.DueDate'][i], '%Y-%m-%d')).days

            obj_row['type'] = data['type'][i]
            obj_row['total_amount'] = float(data['data.Total'][i])
            obj_row['amount_paid'] = float(data['data.AmountPaid'][i])
            obj_row['amount_unpaid'] = float(data['data.AmountDue'][i])

            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                                        ignore_index=True)
    else:
        print("WARNING - Invoices data for business - " + business_uuid + " is not available.")

    return result_df.fillna(0)


def mapBalanceSheetDFNew(business_uuid, data):
    if data is not None or len(data) != 0:
        result_df = pd.DataFrame()

        for i in range(len(data)):
            distinct_columns = set()
            obj_row = {}

            distinct_columns.add('issue_date')
            obj_row['issue_date'] = data[i]['issued_at']

            dataRow = json_normalize(data[i]['data'])['Rows']
            for element in dataRow:
                if isinstance(element, list):
                    dataRow_Cells = json_normalize(element)['Cells']
                    for k in range(len(dataRow_Cells)):
                        distinct_columns.add(json_normalize(dataRow_Cells[k])['Value'][0])
                        obj_row[json_normalize(dataRow_Cells[k])['Value'][0]] = float(
                            json_normalize(dataRow_Cells[k])['Value'][1])

            # Combine into result DF
            result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                         ignore_index=True, sort=True)
    else:
        print("WARNING - Balance sheets data for business - " + business_uuid + " is not available.")

    return result_df.fillna(0)


def aging_list(df):

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
        for ind in range(0,len(nameList)):
            ind1 = np.array([1,1,1,1,1,1,1])
            due_amount = [df['amount_unpaid'].values[i] if pd.Series(ind1*(df.aging.values[i]>=timeList)).sum() == ind+1 else 0.0 for i in range(0, df.shape[0])]
            agingList[nameList[ind]] = due_amount
        agingList = agingList.groupby(['contact']).sum().sort_values(by = 'amount_unpaid', ascending = False)
        #add summary row
        agingList['amount_unpaid_ratio'] = agingList['amount_unpaid']/agingList['amount_unpaid'].sum()
    except Exception as e:
        print('\33[31m','ERROR::aging_list::',e,'\033[0m')
    print('----------')
    return agingList