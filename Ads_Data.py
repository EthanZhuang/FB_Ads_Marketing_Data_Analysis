from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import csv
import time

# Initialize the access_token
access_token = 'EAAJqMzKi2IsBAJp9LxmjKZBfEG5P5WXqzUO8f4eXoUYRr9beYIxSRXKFuXMUTVZCMaJnwewurtWpq93qiyB7X8wfDotZCQZC5M1B9YZAgIuiTEY5cHgOZAhmxGpOin6DM0HZAPcAmzs8YDBnAs4YzS9D1D5jZCLWLUkdhGVhbmddkAZDZD'
ad_account_id = 'act_132799196821088'
app_secret = 'b3700e4944af0e889dc572179bd6800d'
app_id = '679718078830731'

FacebookAdsApi.init(access_token = access_token)

# Insight API中的fields資料(控制需要的欄位資料)
fields = [
    'spend',
    'campaign_name',
    'adset_name',
    'campaign_id',
    'actions',
    'objective',
    'action_values',  
    'conversions',
    'conversion_values',
]
# -----------------------------------------------------------

# 先確立到底要執行多少次迴圈(- 4weeks)
# obj_time = input('Enter the time-start>> YYYY-MM-DD: ')

# 建立CSV資料，並將Business SDK獲得的資料儲存
outfn = '{}_fb.csv'.format('FB_Ads_Marketing_Data')


# 開始時間
week_minus = timedelta(weeks = 1)
start_time = datetime.now() - week_minus
end_time = start_time + relativedelta(months = -1)

for i in range(2):
    # 用於主程式的 time_range
    # start_time.strftime('%Y-%m-%d')

    # Insight API中的fields資料(控制日期條件、資料的層級)
    params = {
        'time_range': {
                    'since': end_time.strftime('%Y-%m-%d'),\
                    'until': start_time.strftime('%Y-%m-%d') 
        }, 

        'level': {'campaign'},
        'time_increment': '1',

        # Default 歸因(目前確認和廣告面板上面的相同 7天點擊 + 7天瀏覽)
        # 'use_account_attribution_setting': 'True',

        # 自己設定的歸因(和廣告預設值有落差)
        'action_attribution_windows': {'7d_click'},
    }
    # -----------------------------------------------------------


    acc_insights = AdAccount(ad_account_id).get_insights(
        fields = fields,
        params = params,
    )

    # 寫入資料庫-----------------------------------------------------
    # 日報
    # outfn = '{%Y-%m-%d}.database.csv'.format(datetime.today())
    run_time = time.time()


    title_row = ['Campaign_name', 'Objective', 'Amount_spent(NTD)', \
                 'Amount_install', \
                 'Amount_purchase' , 'Purchase_Conversion_Value',\
                 'Amount_subscribe', 'Subscribe_Conversion_Value', \
                 'Reporting_date']

    begin_count = 0

    if begin_count == 0:
        with open(outfn, 'w', newline = '') as csvout:
            csvWriter = csv.writer(csvout)
            csvWriter.writerow(title_row)
            
            begin_count += 1
            for acc_insight in acc_insights:     
                Campaign_name = ''
                Amount_spent = ''
                Objective = ''
                Amount_subscribe = '' 
                Subscribe_Conversion_Value = ''
                Amount_install = '' 
                Amount_purchase = ''    
                Purchase_Conversion_Value = ''
                Reporting_date = ''

                # insights/campaign_name得到 "產品包名稱"
                if 'campaign_name' in acc_insight:
                    Campaign_name = acc_insight['campaign_name']

                # insights/spend得到 "總支出成本"
                if 'spend' in acc_insight:    
                    Amount_spent = acc_insight['spend']

                # insights/objective得到 "廣告目標"
                if 'objective' in acc_insight:
                    Objective = acc_insight['objective']

                # insights/date_stop得到 "報表終止日期"
                if 'date_stop' in acc_insight:
                    Reporting_date = acc_insight['date_stop']

                # insights/actions得到 "軟體下載數/課程購買數"
                if 'actions' in acc_insight:
                    for action in acc_insight['actions']:
                        if action['action_type'] == 'mobile_app_install': 
                            Amount_install = action['value']
                        if action['action_type'] == 'purchase':
                            Amount_purchase = action['value']

                # insights/action_values得到 "購買轉換價值"
                if 'action_values' in acc_insight:
                    for ac_values in acc_insight['action_values']:
                        if ac_values['action_type'] == 'omni_purchase':
                            Purchase_Conversion_Value = ac_values['value']

                # insights/conversions得到 "訂閱數"
                if 'conversions' in acc_insight:
                    for cv in acc_insight['conversions']:
                        if cv['action_type'] == 'subscribe_total':
                            try:
                                Amount_subscribe = cv['7d_click']
                            except:
                                Amount_subscribe = cv['value']

                # insights/conversion_values得到 "訂閱轉換價值"             
                if 'conversion_values' in acc_insight:
                    for cv_values in acc_insight['conversion_values']:
                        if cv_values['action_type'] == 'subscribe_total':
                            try:
                                Subscribe_Conversion_Value = cv_values['7d_click']
                            except:
                                Subscribe_Conversion_Value = cv_values['value']

                csvWriter.writerow([
                    Campaign_name,
                    Objective,
                    Amount_spent,
                    Amount_install, 
                    Amount_purchase,
                    Purchase_Conversion_Value,
                    Amount_subscribe,
                    Subscribe_Conversion_Value,
                    Reporting_date,
                ])

                #列印出資料數值，給予比對             
                print(acc_insight['campaign_name'] + '\n', acc_insight)
        
    else:
        with open(outfn, 'a+', newline = '') as csvout:
            csvWriter = csv.writer(csvout)
            
            begin_count += 1
            for acc_insight in acc_insights:     
                Campaign_name = ''
                Amount_spent = ''
                Objective = ''
                Amount_subscribe = '' 
                Subscribe_Conversion_Value = ''
                Amount_install = '' 
                Amount_purchase = ''    
                Purchase_Conversion_Value = ''
                Reporting_date = ''

                # insights/campaign_name得到 "產品包名稱"
                if 'campaign_name' in acc_insight:
                    Campaign_name = acc_insight['campaign_name']

                # insights/spend得到 "總支出成本"
                if 'spend' in acc_insight:    
                    Amount_spent = acc_insight['spend']

                # insights/objective得到 "廣告目標"
                if 'objective' in acc_insight:
                    Objective = acc_insight['objective']

                # insights/date_stop得到 "報表終止日期"
                if 'date_stop' in acc_insight:
                    Reporting_date = acc_insight['date_stop']

                # insights/actions得到 "軟體下載數/課程購買數"
                if 'actions' in acc_insight:
                    for action in acc_insight['actions']:
                        if action['action_type'] == 'mobile_app_install': 
                            Amount_install = action['value']
                        if action['action_type'] == 'purchase':
                            Amount_purchase = action['value']

                # insights/action_values得到 "購買轉換價值"
                if 'action_values' in acc_insight:
                    for ac_values in acc_insight['action_values']:
                        if ac_values['action_type'] == 'omni_purchase':
                            Purchase_Conversion_Value = ac_values['value']

                # insights/conversions得到 "訂閱數"
                if 'conversions' in acc_insight:
                    for cv in acc_insight['conversions']:
                        if cv['action_type'] == 'subscribe_total':
                            try:
                                Amount_subscribe = cv['7d_click']
                            except:
                                Amount_subscribe = cv['value']

                # insights/conversion_values得到 "訂閱轉換價值"             
                if 'conversion_values' in acc_insight:
                    for cv_values in acc_insight['conversion_values']:
                        if cv_values['action_type'] == 'subscribe_total':
                            try:
                                Subscribe_Conversion_Value = cv_values['7d_click']
                            except:
                                Subscribe_Conversion_Value = cv_values['value']

                csvWriter.writerow([
                    Campaign_name,
                    Objective,
                    Amount_spent,
                    Amount_install, 
                    Amount_purchase,
                    Purchase_Conversion_Value,
                    Amount_subscribe,
                    Subscribe_Conversion_Value,
                    Reporting_date,
                ])

                #列印出資料數值，給予比對             
                print(acc_insight['campaign_name'] + '\n', acc_insight)
            start_time += relativedelta(months = -1) 
            end_time += relativedelta(months = -1)

            # flag確立program完整結束
            print('finish_{}'.format(begin_count))       
            print('--- %s seconds ---' % (time.time() - run_time))
print('finish End')