# %%
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import csv
import time
import pandas as pd
import numpy as np
import pyodbc
import logging
# %%

class FbAds_Data(object):

    # 連線SQL
    FbAds_Data_attri = 0
    DBdata = {'ip':'192.168.99.142', 'db':'CMAPP'}
    user142 = 'cmapp'
    pwd142 = '0000'
    conn_DBdata = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DBdata['ip'] + \
                ';DATABASE=' + DBdata['db'] + ';UID=' + user142 + ';PWD=' + pwd142)
    DBdata_cr = conn_DBdata.cursor()
    TableName = 'FB_AdsData'

    def __init__(self,)
# %%
if __name__ == '__main__':


# %% [markdown]
# ## Heading 1

# %%
