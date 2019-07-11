from datetime import datetime
import json
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression

BANK_TRANSACTION_DEMO_JSON = './limit_engine/TestData/bank_transaction_demo.json'
PAYMENTS_DEMO_JSON = './limit_engine/TestData/payments_demo.json'

def loadXeroBankTransactionDemo(filePath):
    data = None
    with open(filePath) as f:
        data = json.load(f)
    return data


def getDaysBucket(updatedDate):
    days = (datetime.now()-updatedDate).days
    if(days>=0 and days<30):
        return '1.[0-30]'
    if(days>=30 and days<60):
        return '2.[30-60]'
    if(days>=60 and days<90):
        return '3.[60-90]'
    if(days>=90 and days<180):
        return '4.[90-180]'
    if(days>=180):
        return '5.[>180]'

def parseXeroDate(xeroDate):
    updatedDateInt = int(xeroDate[6:19])
    updatedDate = datetime.utcfromtimestamp(updatedDateInt/1e3)#.strftime('%Y-%m-%d')
    return updatedDate

def parseXeroStringDate(xeroStrDate):
    return datetime.strptime(xeroStrDate[0:10],'%Y-%m-%d')

##Calculate teh trend of credit limit and profit margin
# with Ordinary linear fit while ignoring the missing value in monthly revenues
# returns: negative if the trend is down
def getTrendCoefFromSeries(series):
    rowList = []
    lrModel = LinearRegression()
    lrModel.fit(np.array(series.index).reshape(-1,1), np.array(series.values).reshape(-1,1))
    trd_coef = lrModel.coef_[0][0]
    trd_intercept = lrModel.intercept_[0]
    return trd_coef

def plotLineChart(xAxis, yAxis, xLabel, yLabel, title):
    # Data for plotting
    fig, ax = plt.subplots()
    ax.plot(xAxis, yAxis)
    ax.set(xlabel=xLabel, ylabel=yLabel, title=title)
    plt.xticks(rotation = 45)
    ax.grid()
    plt.show()
