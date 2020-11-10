from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
import datetime
import csv

# Set the info to get connected to the API. Do NOT share this info
my_app_id = '<Your app id>'
my_app_secret = '<Your app secret>'
my_access_token = '<user token>'

FacebookAdsApi.init(access_token = access_token)

# # Get today's date for the filename, and the csv data
# today = datetime.datetime.now() 
# todaydate = today.strftime('%m-%d-%Y')
# # todaydatehyphen = todaydate.strftime('%m-%d-%Y')

# # Define the destination filename
# # filename = todaydatehyphen + '_fb.csv'

fields = [
    'spend',
    'campaign_name',
    'adset_name',
    'actions',
    'objective',
    'action_values',   
]
params = {
    'time_range': {'since':'2020-11-01', 'until':'2020-11-08'},
    'filtering': [],
    'level': {'campaign', 'adset'},
    'time_increment': '1',
}

acc_insights = AdAccount(ad_account_id).get_insights(
    fields = fields,
    params = params,
)

# 寫入資料庫-----------------------------------------------------
outfn = '{}_fb.csv'.format("<Your file's name>"")
title_row = ['Campaign_name', 'Adset_name', 'Amount_spent(NTD)', 'Objective', 'Amount_subscribe', 'Amount_install', 'Amount_purchase' , 'Reporting_date']

with open(outfn, 'w', newline = '') as csvout:
    csvWriter = csv.writer(csvout)
    csvWriter.writerow(title_row)

    for acc_insight in acc_insights:             
        Campaign_name = ''
        Adset_name = ''
        Amount_spent = ''
        Objective = ''
        Amount_subscribe = '' 
        Amount_install = '' 
        Amount_purchase = ''    
        Reporting_date = ''

        if 'campaign_name' in acc_insight:
            Campaign_name = acc_insight['campaign_name']
        if 'adset_name' in acc_insight:
            Adset_name = acc_insight['adset_name']
        if 'spend' in acc_insight:
            Amount_spent = acc_insight['spend']
        if 'objective' in acc_insight:
            Objective = acc_insight['objective']
        if 'date_stop' in acc_insight:
            Reporting_date = acc_insight['date_stop']

        if 'actions' in acc_insight:
            for action in acc_insight['actions']:
                if action['action_type'] == 'mobile_app_install': 
                    Amount_install = action['value']

                if action['action_type'] == 'purchase':
                    Amount_purchase = action['value']

                if action['action_type'] == 'subscription': 
                    Amount_subscribe = action['value']
    
        csvWriter.writerow([
            Campaign_name,
            Adset_name,
            Amount_spent,
            Objective,
            Amount_subscribe,
            Amount_install, 
            Amount_purchase,    
            Reporting_date,
        ])
        
print('finish_1')
