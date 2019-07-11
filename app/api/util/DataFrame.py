import pandas as pd

def readCSVFromURL(url):
    if url is None or url is '':
        return None
    else:
        return pd.read_csv(url)
