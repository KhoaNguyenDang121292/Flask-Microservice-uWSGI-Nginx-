import json
import jsonpath_rw_ext as jp
import pandas as pd
import numpy as np
from datetime import datetime
from collections import OrderedDict
import api.util.CommonUtils as limitUtils
import re


def convertToNumber(number):
    number = str(number).replace('.','')
    number = str(number).replace(',','.')
    number = str(number).replace(' ','')
    return float(number.strip())


def mapPnLToDF_New(data_pnl):
    distinct_columns = ['period', 'currency', 'net_operating_income', 'net_income', 'total_comprehensive_income',
                        'total_revenue', 'cost_of_good_sold', 'expense', 'other_income', 'other_expense',
                        'other_comprehensive_income_loss']
    result_df = pd.DataFrame()
    data = data_pnl['data']

    for i in range(len(data)):
        net_income = 0.0
        net_operating_income = 0.0
        total_comprehensive_income = 0.0
        total_revenue = 0.0
        cost_of_good_sold = 0.0
        expense = 0.0
        other_income = 0.0
        other_expense = 0.0
        other_comprehensive_income_loss = 0.0

        obj_row = {}
        header = data[i]['data']['header']

        for j in range(len(header['net_income'])):
            net_income = net_income + convertToNumber(header['net_income'][j]['income'])
        for j in range(len(header['net_operating_income'])):
            net_operating_income = net_operating_income + convertToNumber(header['net_operating_income'][j]['income'])
        for j in range(len(header['total_comprehensive_income'])):
            total_comprehensive_income = total_comprehensive_income + convertToNumber(
                header['total_comprehensive_income'][j]['income'])

        for j in range(len(data[i]['data']['primary_income']['total'])):
            total_revenue = total_revenue + convertToNumber(data[i]['data']['primary_income']['total'][j]['total'])

        for j in range(len(data[i]['data']['cost_of_good_sold']['total'])):
            cost_of_good_sold = cost_of_good_sold + convertToNumber(
                data[i]['data']['cost_of_good_sold']['total'][j]['total'])

        for j in range(len(data[i]['data']['expense']['total'])):
            expense = expense + convertToNumber(data[i]['data']['expense']['total'][j]['total'])

        for j in range(len(data[i]['data']['other_income']['total'])):
            other_income = other_income + convertToNumber(data[i]['data']['other_income']['total'][j]['total'])

        for j in range(len(data[i]['data']['other_expense']['total'])):
            other_expense = other_expense + convertToNumber(data[i]['data']['other_expense']['total'][j]['total'])

        for j in range(len(data[i]['data']['other_comprehensive_income_loss']['total'])):
            other_comprehensive_income_loss = other_comprehensive_income_loss + convertToNumber(
                data[i]['data']['other_expense']['total'][j]['total'])

        obj_row['period'] = header['net_operating_income'][0]['period']
        obj_row['currency'] = header['currency_format']
        obj_row['total_comprehensive_income'] = str(total_comprehensive_income)
        obj_row['net_operating_income'] = str(net_operating_income)
        obj_row['net_income'] = str(net_income)
        obj_row['total_revenue'] = str(total_revenue)
        obj_row['cost_of_good_sold'] = str(cost_of_good_sold)
        obj_row['expense'] = str(expense)
        obj_row['other_income'] = str(other_income)
        obj_row['other_expense'] = str(other_expense)
        obj_row['other_comprehensive_income_loss'] = str(other_comprehensive_income_loss)

        result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                     ignore_index=True)

    return result_df.fillna(0)


def mapPnLToDF(data):
    pnlDF1 = pd.DataFrame()
    pnlDF2 = pd.DataFrame()

    period = pd.Series(jp.match('$.[*].data.header.net_operating_income[*].period', data))
    currency = pd.Series([jp.match1('$.[*].data.header.currency_format', data)[4:7] for i in range(0,len(period))])
    net_operating_income = pd.Series(jp.match('$.[*].data.header.net_operating_income[*].income', data))
    net_income = pd.Series(jp.match('$.[*].data.header.net_income[*].income', data))
    total_comprehensive_income = pd.Series(jp.match('$.[*].data.header.total_comprehensive_income[*].income', data))
    pnlDF1 = pd.concat([period, currency, net_operating_income, net_income, total_comprehensive_income], axis = 1)
    pnlDF1.columns = (['period','currency','net_operating_income','net_income','total_comprehensive_income'])
    
    total_revenue = pd.Series(jp.match('$.[*].data.primary_income.total.[*].total', data))
    cost_of_good_sold = pd.Series(jp.match('$.[*].data.cost_of_good_sold.total.[*].total', data))
    expense = pd.Series(jp.match('$.[*].data.expense.total.[*].total', data))
    other_income = pd.Series(jp.match('$.[*].data.other_income.total.[*].total', data))
    other_expense = pd.Series(jp.match('$.[*].data.other_expense.total.[*].total', data))
    other_comprehensive_income_loss = pd.Series(jp.match('$.[*].data.other_comprehensive_income_loss.total.[*].total', data))
    pnlDF2 = pd.concat([total_revenue, cost_of_good_sold, expense, other_income, other_expense, other_comprehensive_income_loss, period], axis = 1)
    pnlDF2.columns = (['total_revenue', 'cost_of_good_sold', 'expense', 'other_income', 'other_expense', 'other_comprehensive_income_loss','period'])
    pnlDF2 = pnlDF2.set_index('period')
    
    pnlDF = pd.merge(pnlDF1,pnlDF2, left_on = 'period', right_index = True)
    
    floatColumns = ['net_operating_income','net_income','total_comprehensive_income','total_revenue','cost_of_good_sold','expense','other_income','other_expense','other_comprehensive_income_loss']
    for item in floatColumns:
        pnlDF[item] = pnlDF[item].apply(convertIndoCurrencyToNumber)
    
    pnlDF['gross_profit'] = pnlDF.total_revenue - pnlDF.cost_of_good_sold
    pnlDF['profit_margin'] = pnlDF.net_income/pnlDF.total_revenue
    pnlDF.sort_index(ascending=False,inplace=True)
    return pnlDF.fillna(0)


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
    if df is None:
        return pd.DataFrame()
    if df.shape[0] == 0:
        return pd.DataFrame()
    clients_cctn = pd.DataFrame(df.groupby('contact')['amount_unpaid'].sum())
    clients_cctn = clients_cctn[clients_cctn['amount_unpaid'] != 0]
    clients_cctn['% as of total'] = clients_cctn['amount_unpaid'] / clients_cctn['amount_unpaid'].sum()
    clients_cctn = clients_cctn.sort_values(by='amount_unpaid', ascending=False)
    return clients_cctn


def aging_list(df):
    agingList = pd.DataFrame()
    if (df is None or df.shape[0] == 0):
        return pd.DataFrame()
    try:
        agingList['contact'] = df.contact
        agingList['amount_unpaid'] = df.amount_unpaid

        nameList = ['1<>30', '31<>60', '61<>90', '91<>120', '121<>180', '181<>360', '>360']
        timeList = [0, 31, 61, 91, 121, 181, 361]
        for ind in range(0, len(nameList)):
            ind1 = np.array([1, 1, 1, 1, 1, 1, 1])
            due_amount = [df['amount_unpaid'].values[i] if pd.Series(
                ind1 * (df.aging.values[i] >= timeList)).sum() == ind + 1 else 0.0 for i in range(0, df.shape[0])]
            agingList[nameList[ind]] = due_amount
        agingList = agingList.groupby(['contact']).sum().sort_values(by='amount_unpaid', ascending=False)
        # add summary row
        agingList['amount_unpaid_ratio'] = agingList['amount_unpaid'] / agingList['amount_unpaid'].sum()
    except Exception as e:
        print('\33[31m', 'ERROR::aging_list::', e, '\033[0m')
    return agingList

def getPurchaseInvoicesDF(allInvoicesDF):
    if allInvoicesDF is None:
        return pd.DataFrame()
    if allInvoicesDF.shape[0] == 0:
        return pd.DataFrame()
    payableInvoicesDF = allInvoicesDF[allInvoicesDF['type']=='ACCPAY']
    return payableInvoicesDF

def getSalesInvoicesDF(allInvoicesDF):
    if allInvoicesDF is None:
        return pd.DataFrame()
    if allInvoicesDF.shape[0] == 0:
        return pd.DataFrame()
    salesInvoicesDF = allInvoicesDF[allInvoicesDF['type']=='ACCREC']
    return salesInvoicesDF

def mapInvoicesDF(data):
    if len(data['data']) == 0:
        print('No biz data is actually returned')
        return pd.DataFrame()
    else:
        invoicesDF = pd.DataFrame()
        for i in range(0, len(data['data'])):
            # print(i)
            rowList = []
            row = OrderedDict()

            row['contact'] = jp.match1('$.data.[' + str(i) + '].contact', data)
            row['type'] = jp.match1('$.data.[' + str(i) + '].data.Type', data)
            row['currency'] = jp.match1('$.data.[' + str(i) + '].currency', data)
            row['status'] = jp.match1('$.data.[' + str(i) + '].status', data)
            row['reference'] = jp.match1('$.data.[' + str(i) + '].data.Reference', data)
            # print('--------------------=',jp.match1('$.['+str(i)+'].data.Date', data))
            issued_date = datetime.strptime(jp.match1('$.data.[' + str(i) + '].data.Date', data), '%Y-%m-%d')
            row['issued_date'] = issued_date
            if (jp.match1('$.data.[' + str(i) + '].data.DueDate', data) is None) == False:
                due_date = datetime.strptime(jp.match1('$.data.[' + str(i) + '].data.DueDate', data), '%Y-%m-%d')
                time_today = datetime.now()
                date_diff = (due_date - issued_date).days
                row['due_date'] = due_date
                row['term'] = date_diff
                row['aging'] = (time_today - row['due_date']).days

            row['total_amount'] = float(jp.match1('$.data.[' + str(i) + '].data.Total', data))
            # row['subTotal'] = float(jp.match1('$.data.['+str(i)+'].data.SubTotal', data))
            if (jp.match1('$.data.[' + str(i) + '].data.AmountPaid', data) is None) == False:
                row['amount_paid'] = float(jp.match1('$.data.[' + str(i) + '].data.AmountPaid', data))
            row['amount_unpaid'] = float(jp.match1('$.data.[' + str(i) + '].data.AmountDue', data))

            rowList.append(row)
            invoicesDF = invoicesDF.append(rowList, ignore_index=True)

        COLUMN_NAMES = ['contact', 'currency', 'issued_date', 'due_date', 'aging', 'type', 'term', 'total_amount',
                        'amount_paid', 'amount_unpaid']
        invoicesDF = invoicesDF[COLUMN_NAMES]
        return invoicesDF.fillna(float(0))


def mapInvoicesToDF_New(data):
    distinct_columns = set(
        ['issued_time', 'currency', 'amount', 'contact', 'created_time', 'term_name', 'first_pmt_date',
         'received_amount'])
    result_df = pd.DataFrame()
    data = data['data']
    for i in range(len(data)):
        obj_row = {}
        obj_row['issued_time'] = data[i]['issued_at']
        obj_row['currency'] = data[i]['currency']
        obj_row['amount'] = data[i]['amount']
        obj_row['contact'] = data[i]['contact']
        data_data = data[i]['data']
        obj_row['created_time'] = data_data['due_date']
        obj_row['term_name'] = data_data['term']['name']
        obj_row['first_pmt_date'] = data_data['earliest_payment_date']
        obj_row['received_amount'] = data_data['payment_received_amount_currency_format']
        for j in range(len(data_data['transaction_lines_attributes'])):
            distinct_columns.add('payment_' + str(j) + '_amount')
            obj_row['payment_' + str(j) + '_amount'] = data_data['transaction_lines_attributes'][j]['amount']

        # Combine into result DF
        result_df = result_df.append(pd.DataFrame(obj_row, columns=list(distinct_columns), index=[i]),
                                     ignore_index=True)

    return result_df.fillna(0)

def mapInvoicesToDF(data):
    if len(data['data']) is 0 or data is None or data['data'] is None:
        print('No data return.')
        return pd.DataFrame()
    else:
        invoicesDF = pd.DataFrame()
        for i in range(0,len(data)):
            listDF = pd.DataFrame()
            rowList = []
            row = OrderedDict()

            row['term_name'] = jp.match1('$.['+ str(i) +'].data.term.name', data)
            row['contact'] = jp.match1('$.['+ str(i) +'].contact', data)
            row['amount'] = jp.match1('$.['+ str(i) +'].amount',data)/100
            row['currency'] = jp.match1('$.['+ str(i) +'].currency', data)

            row['issued_time'] = datetime.strptime(jp.match1('$.['+ str(i) +'].issued_at.date', data)[0:10],'%Y-%m-%d')
            row['created_time'] = datetime.strptime(jp.match1('$.['+ str(i) +'].data.due_date', data),'%d/%m/%Y')
            row['due_date'] = jp.match1('$.['+ str(i) +'].data.created_at', data)[:10]
            if jp.match1('$.['+ str(i) +'].data.earliest_payment_date', data) == '':
                row['first_pmt_date'] = 'None'
            else:
                row['first_pmt_date'] = datetime.strptime(jp.match1('$.['+ str(i) +'].data.earliest_payment_date', data)[0:10],'%d/%m/%Y')

            for ind in range(0,10):
                row['payment_0'+ str(ind) + '_amount'] = jp.match1('$.['+ str(i) +'].data.transaction_lines_attributes.['+ str(ind) +'].amount', data)
            for ind in range(10,len(data[i]['data']['transaction_lines_attributes'])):
                row['payment_'+ str(ind) + '_amount'] = jp.match1('$.['+ str(i) +'].data.transaction_lines_attributes.['+ str(ind) +'].amount', data)
            row['received_amount'] = jp.match1('$.['+ str(i) +'].data.payment_received_amount_currency_format', data)[5:]

            rowList.append(row)
            listDF = listDF.append(rowList, ignore_index = True)
            invoicesDF = pd.concat([invoicesDF, listDF], sort = True)

            #time_today = datetime.now()
            #date_diff = (due_date - issue_date).days
            #ovde = (time_today - due_date).days
            #jp.match1('$.['+str(5)+'].data.payment_received_amount_currency_format', data_iov)[5:]

            #rowList.append(row)
            #invoicesDF = invoicesDF.append(rowList, ignore_index=True)
        invoicesDF.received_amount = invoicesDF.received_amount.apply(convertIndoCurrencyToNumber)
        return invoicesDF.fillna(0)

def mapBalanceSheetDF(data):
    if len(data['data']) == 0:
       print('No biz data is actually returned')
       return pd.DataFrame()
    else:
        bsDF = pd.DataFrame()
        for i in range(0,len(data['data'])):
            rowList = []
            rows = OrderedDict()
            #rows['code'] = data['data'][i]['code']
            rows['date'] = datetime.strptime(re.sub(' ','-',data['data'][i]['data'][0]['Cells'][1]['Value']),'%d-%b-%Y')
            #rows['currency'] = data['data'][i]['currency']
            #rows['status'] = data['data'][i]['status']
            #rows['type'] = data['data'][i]['type']

            names = jp.match('$.data.['+str(i)+'].data.[*].Rows.[*].Cells.[*].Value', data)[::3]
            values = jp.match('$.data.['+str(i)+'].data.[*].Rows.[*].Cells.[*].Value', data)[1::3]
            for ind in range(0,len(names)):
                rows[names[ind]] = float(values[ind])
            if ('Total Assets' in list(rows.keys())):
                rowList.append(rows)
                bsDF = bsDF.append(rowList)
        if pd.Series(list(bsDF.shape)).sum() == 0:
            print('mapBalanceSheetDF::No actual data is returned')
            return pd.DataFrame()
        else:
            bsDF = bsDF.set_index('date')
            return(bsDF.fillna(0))

def mapBankTransactionsToDF(data):
    banktrxDF = pd.DataFrame()
    for i in range(0,len(data['BankTransactions'])):
        rowList = []
        row = OrderedDict()
        recType = data['BankTransactions'][i]['Type']
        row['Type'] = recType
        row['Bank Account - Name'] = data['BankTransactions'][i]['BankAccount']['Name']
        row['Contact - Name'] = data['BankTransactions'][i]['Contact']['Name']
        trxRef = jp.match1('$.BankTransactions['+str(i)+'].Reference', data)
        if trxRef is None:
            trxRef = ''
        row['Ref'] = trxRef
        #row['Total'] = data['BankTransactions'][i]['Total']
        # I took sub total as it is amount after tax. Is more conservative
        subtotal = data['BankTransactions'][i]['SubTotal']
        if recType=='SPEND':
            subtotal = -subtotal
        row['Amount'] = subtotal
        
        trxDate = limitUtils.parseXeroStringDate(data['BankTransactions'][i]['DateString'])
        row['Date'] = trxDate
        row['Month'] = trxDate.strftime('%m-%Y')
        row['Currency Code'] = data['BankTransactions'][i]['CurrencyCode']
        
        
        row['Bank Account - Name'] = data['BankTransactions'][i]['BankAccount']['Name']
        row['Is Reconciled'] = data['BankTransactions'][i]['IsReconciled']
        #print(trxType,'\t',trxTotal, '\t',trxIsReconciled)

        rowList.append(row)
        banktrxDF = banktrxDF.append(rowList, ignore_index=True)

    return banktrxDF


def mapPaymentToDF(data):
    banktrxDF = pd.DataFrame()
    for i in range(0,len(data['BankTransactions'])):
        rowList = []
        row = OrderedDict()
        recType = data['BankTransactions'][i]['Type']
        row['Type'] = recType
        row['Bank Account - Name'] = data['BankTransactions'][i]['BankAccount']['Name']
        row['Contact - Name'] = data['BankTransactions'][i]['Contact']['Name']
        trxRef = jp.match1('$.BankTransactions['+str(i)+'].Reference', data)
        if trxRef is None:
            trxRef = ''
        row['Ref'] = trxRef
        #row['Total'] = data['BankTransactions'][i]['Total']
        # I took sub total as it is amount after tax. Is more conservative
        subtotal = data['BankTransactions'][i]['SubTotal']
        if recType=='SPEND':
            subtotal = -subtotal
        row['Amount'] = subtotal
        
        trxDate = limitUtils.parseXeroStringDate(data['BankTransactions'][i]['DateString'])
        row['Date'] = trxDate
        row['Month'] = trxDate.strftime('%m-%Y')
        row['Currency Code'] = data['BankTransactions'][i]['CurrencyCode']
        
        
        row['Bank Account - Name'] = data['BankTransactions'][i]['BankAccount']['Name']
        row['Is Reconciled'] = data['BankTransactions'][i]['IsReconciled']
        #print(trxType,'\t',trxTotal, '\t',trxIsReconciled)

        rowList.append(row)
        banktrxDF = banktrxDF.append(rowList, ignore_index=True)

    return banktrxDF

def get_pnl_cogs_items(data):
    pnlDF = pd.DataFrame()
    for ind in range(0,len(data)):
        amount = pd.Series([float(re.sub(',','.',re.sub(r'[ .]','',ind))) for ind in jp.match('$.['+ str(ind) +'].data.cost_of_good_sold.accounts[*].array[*].data[*].balance', data)])
        item = pd.Series(jp.match('$.['+ str(ind) +'].data.cost_of_good_sold.accounts[*].array[*].name', data))
        rowList = pd.DataFrame(amount).T
        rowList.columns = item
        pnlDF = pd.concat([pnlDF, rowList], sort = True)
    
    pnlDF.index = pd.date_range(start = jp.match1('$.['+ str(len(data)-1) + '].[issued_at].[date]',data)[0:10], end = jp.match1('$.[0].[issued_at].[date]',data)[0:10], freq = 'MS')[-1::-1]
    
    return pnlDF.fillna(0).T

def get_expense_items(data):
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
