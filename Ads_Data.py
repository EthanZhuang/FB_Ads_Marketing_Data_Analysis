from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import csv
import time

# Initialize the access_token
access_token = 
ad_account_id = 
app_secret = 
app_id = 

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

week_add = relativedelta(weeks = 1)
day_add = relativedelta(days = 1)

# 回溯到6個月以前，定義data_back = 6
data_back = 6

# 現在時間回推7天
end_time = datetime.now() - week_add - (data_back - 1)*relativedelta(months = 1)
# 從甚麼時候
start_time = datetime.now() - week_add - data_back*relativedelta(months = 1)
begin_count = 0

# 建立CSV資料，並將Business SDK獲得的資料儲存
outfn_t = '{}_fb.txt'.format('Test_FB_Ads_Marketing_Data')


title_row = ['Campaign_name' + '\t' + 'Objective' + '\t' + \
             'Amount_spent(NTD)' + '\t' + 'Amount_install'  + '\t' +\
             'Amount_purchase' + '\t' + 'Purchase_Conversion_Value'  + '\t' +\
             'Amount_subscribe' + '\t' + 'Subscribe_Conversion_Value' + '\t' +\
             'Reporting_date']

with open(outfn_t, 'w', encoding = 'utf-8') as textout:
        textout.write(','.join(title_row) + '\n')


# 開始將資料一個月一個月寫入資料庫
for i in range(6):
    # Insight API中的fields資料(控制日期條件、資料的層級)
    params = {
        'time_range': {
                    'since': start_time.strftime('%Y-%m-%d'),\
                    'until': end_time.strftime('%Y-%m-%d') 
        }, 

        'level': {'campaign'},
        'time_increment': '1',

        # Default 歸因(目前確認和廣告面板上面的相同 7天點擊 + 7天瀏覽)
        # 'use_account_attribution_setting': 'True',

        # 自己設定的歸因(和廣告預設值有落差)
        'action_attribution_windows': {'7d_click'},
    }

    acc_insights = AdAccount(ad_account_id).get_insights(
        fields = fields,
        params = params,
    )

    # 寫入資料庫-----------------------------------------------------
    run_time = time.time()


#     with open(outfn, 'a', newline = '') as csvout:
#         csvWriter = csv.writer(csvout)
    with open(outfn_t, 'a', encoding = 'utf-8') as textout:
        
        start_time = start_time + day_add + relativedelta(months = 1)
        end_time = end_time + relativedelta(months = 1)
        
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

            try:
                stuff = [Campaign_name + '\t' + Objective + '\t' + Amount_spent \
                         + '\t' + Amount_install + '\t' + Amount_purchase\
                         + '\t' + Purchase_Conversion_Value + '\t' + Amount_subscribe\
                         + '\t' + Subscribe_Conversion_Value + '\t' + Reporting_date]
                textout.write(','.join(stuff) + '\n')
            except UnicodeEncodeError as e:
                print(e)
                textout.write(' ' + '\n')
           
            # 列印出資料數值，給予比對             
#             print(acc_insight['campaign_name'] + '\n', acc_insight)
        print('第{}次'.format(i)) 
        time.sleep(100)

# flag確立program完整結束
print('--- %s seconds ---' % (time.time() - run_time))