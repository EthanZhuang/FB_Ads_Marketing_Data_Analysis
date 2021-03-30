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
import traceback, sys
import smtplib
from email.mime.text import MIMEText
import socket

# %%
# 連線SQL
DBdata = {'ip':'192.168.99.142', 'db':'CMAPP'}
user142 = 'cmapp'
pwd142 = '0000'
conn_DBdata = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DBdata['ip'] + \
              ';DATABASE=' + DBdata['db'] + ';UID=' + user142 + ';PWD=' + pwd142)
DBdata_cr = conn_DBdata.cursor()
TableName = 'FB_AdsData'
# %% 
# 紀錄程式跑多久時間
run_time = time.time()

# 提取流水號、報告日期
def Get_SN_RD(TableName):
    get_both = """
        SELECT [Serial_Number], [Reporting_Date]
        FROM [CMAPP].[dbo].[{}]
        WHERE  [Serial_Number] = (SELECT MAX(Serial_Number) 
        FROM [CMAPP].[dbo].[{}])
        """
    print(get_both.format(TableName, TableName))
    DBdata_cr.execute(get_both.format(TableName, TableName))
    Both_SN_RD = DBdata_cr.fetchall()
    conn_DBdata.commit()
    return Both_SN_RD

# 清洗時間(Reporting_time)欄位，並更新
def Clean_both_SN_RD(uncleaned_data):
    index = 0
    list_of_data = [k for k in str(uncleaned_data).strip('[]').strip('()').split(',')]
    for i in list_of_data:
#         print(i.strip().strip('Decimal').strip('()').strip("'"), '\n')
        cleaned_data = i.strip().strip('Decimal').strip('()').strip("'")
        
        # 將清洗好的資料儲存
        if index%2 == 0:
            index += 1
            try:
                cleaned_SN = int(float(cleaned_data))
            except ValueError as e_reporting_date:
                print(e_reporting_date)
            
        elif index%2 == 1: 
            index += 1
            try:
                cleaned_RD = int(float(cleaned_data))
            except ValueError as e_reporting_date:
                print(e_reporting_date)
    return cleaned_SN, cleaned_RD

# 將Reporting_Date回溯1個月前
def RD_date_back(cleaned_RD):
    # 將str變成時間格式(datetime)，才能做計算
    reporting_date = datetime.strptime(str(cleaned_RD), '%Y%m%d')
    
    # 當原始資料內的日期 < 現在日期減一個月，代表資料需要的量是超過一個月
    if reporting_date.date() < (datetime.now().date() - relativedelta(months = 1)):
        reporting_date = reporting_date.date()
    else:
    # 當原始資料內的日期 < 現在日期減一個月，代表資料需要回溯一個月
        reporting_date = reporting_date.date() - relativedelta(months = 1)
    return reporting_date


# 增加流水號 & 廣告類別
def Expand_df(df, cleaned_SN): 
    # 第幾排插入流水號 
    idx_SN = 0
    idx_CA = 15
    Serial_Number = []
    # 廣告類別
    Category = 'FB'

    # 增加Serial_Number & Catergory(FB, GOOGL, AAPL)
    for order in range(len(df)):
        s_num = str(df.Reporting_Date[order]) + '000' + str(int(str(cleaned_SN)[8:]) + order + 1)
        Serial_Number.append(int(s_num))
    
    # 將資料插入
    df.insert(loc = idx_SN, column = 'Serial_Number', value = Serial_Number)
    df.insert(loc = idx_CA, column = 'Category', value = Category)
    return df

# 將CSV內檔案的
def Blank_to_None(df):
    for col in df.columns:
        df[col] = df[col].apply(lambda x: None if x == '' else x)
    return df 

# 將df的資料備份下來
def Df_to_Csv(df):
    # 將df內的Data儲存成CSV file
    df.to_csv('{}_FBAdsTest.csv'.format(datetime.now().strftime('%Y%m%d')), index = False, encoding='utf_8_sig')
    return None

# 刪除資料
def DelSQL(TableName, start_time, now_time):
    str_query = """
        DELETE
        FROM [CMAPP].[dbo].[{}]
        WHERE Reporting_Date BETWEEN {} AND {}
        """
    print(str_query.format(TableName, start_time, now_time))
    DBdata_cr.execute(str_query.format(TableName, start_time, now_time))
    conn_DBdata.commit()   

# 寫入資料
def InsertSQL(df, TableName):
    str_query = """
        INSERT INTO [CMAPP].[dbo].[{}]([Serial_Number],[Campaign_Name],[Adset_Name],[Ad_Name],[Objective],[Amount_Spent],[Amount_Install],\
        [Amount_Purchase],[Purchase_Conversion_Value],[Amount_Subscribe],[Subscribe_Conversion_Value],[CPM],[CTR],[Impressions],[Reporting_Date]\
        ,[Category])VALUES({})
        """
    # 將csv DataFrame每筆資料變成list形式
    insert_data = df.values.tolist()
    for i in range(len(insert_data)):
#         print(str_query.format(TableName, str(insert_data[i]).strip('[]').replace('None', 'null')))      
        DBdata_cr.execute(str_query.format(TableName, str(insert_data[i]).strip('[]').replace('None', 'null')))
        conn_DBdata.commit() 

def send_email(Text):

    socket.getaddrinfo('127.0.0.1', 8080)

    MailSender = 'ai@cmoney.tw'
    MailSenderPwd = '1qaz@wsx'
    MailReceivers = ['love824671@gmail.com','ethanzhuang824671@gmail.com']
    MailText = Text
    MailContents = MIMEText(MailText,'Plain','utf-8')

    if 'well' in Text:
        MailContents['Subject'] = '成本自動化(完成)'
    else:
        MailContents['Subject'] = '成本自動化(失敗)'
    MailContents['From'] = 'ai@cmoney.tw'

    MailObj = smtplib.SMTP(host = 'smtp.gmail.com', port = '587')
    MailObj.ehlo()
    MailObj.starttls()
    MailObj.login(MailSender, MailSenderPwd)
    MailObj.sendmail(MailSender,MailReceivers,MailContents.as_string())
    MailObj.quit()

# %%
# FB SDKs爬蟲、資料整理、資料表格化
#  
#
def main_prog(start_time, end_time, now_time):
    # 起始值
    """
    access_token: 廣告金鑰
    ad_account_id: 廣告帳戶
    data_back: 往前爬多久的資料(幾個月)
    fields: FB_Ads行銷廣告資料
    count: 計算整體while迴圈跑幾次
    stop_flag: 當結束時間已經超過現在時間，使迴圈再跑一次，然後停止迴圈
    """
    access_token = 'EAAJqMzKi2IsBAHrHkHKYzF8Kc1PgskkXweJlxbv2qtNDrIVWdIHPXZBuclZAdFVuCE9tjZAfklwL8JxOmdhcZABjKYJRu9kqXMq3Q2kwBWmdZBjixZCCEkTzMXlrHeW9f3eJoylClpa0FqMUbecNair0MhT74eYZAJtwQxL5aoF4mejBTn2ZBAz6S4FSP6pUy2gZD'
    ad_account_id = 'act_132799196821088'
    count = 0
    stop_flag = 0
    FacebookAdsApi.init(access_token = access_token)

    # Insight API中的fields資料(控制需要的欄位資料)
    fields = [
        'spend',
        'campaign_name',
        'adset_name',
        'campaign_id',
        'ad_name',
        'actions',
        'objective',
        'action_values',  
        'conversions',
        'conversion_values',
        'impressions',
        'ctr',
        'cpm',
    ]

    # 起始資料放置位置
    Campaign_Name = list()
    Adset_Name = list()
    Ad_Name = list()
    Objective = list()
    Amount_Spent = list()
    Amount_Install = list()
    Amount_Purchase = list()
    Purchase_Conversion_Value = list()
    Amount_Subscribe = list()
    Subscribe_Conversion_Value = list()
    Reporting_Date = list()
    CPM = list()
    CTR = list()
    Impressions = list()

    # 起始時間、結束時間、現在爬蟲時間
    start_time, end_time, now_time = start_time, end_time, now_time.date()

    # 比較時間前後順序
    while True:
        # 計算迴圈跑幾次
        count += 1
        # Insight API中的fields資料(控制日期條件、資料的層級)
        # start_time和end_time需要為string(%Y-%m-%d)
        params = {
            'time_range': {
                        'since': start_time.strftime('%Y-%m-%d'),\
                        'until': end_time.strftime('%Y-%m-%d'), 
            }, 
            # level從campaign改成ad
            'level': {'ad'},
            'time_increment': '1',
            # 手動設定的歸因: 7天點擊後(和廣告預設值有落差)
            'action_attribution_windows': {'7d_click'},
        }

        # 廣告帳號獲取廣告Insights資訊
        acc_insights = AdAccount(ad_account_id).get_insights(
            fields = fields,
            params = params,
        )

        # 了解時間怎麼走
        print(start_time.strftime('%Y-%m-%d'))
        print(end_time.strftime('%Y-%m-%d'))
        
        # stop_flag = 0 代表end_time的時間已經超過現在時間(now_time)
        if stop_flag == 0:
        # 起始時間(start_time)和結束時間(end_time)，一次取一星期
            start_time = end_time + relativedelta(days = 1)
        else:
            start_time = end_time
        
        # 每次都將結束時間(end_time)，往後增加一星期
        end_time = end_time + relativedelta(weeks = 1)

        # 從廣告Inishgts資訊中，清洗、篩選資料
        for acc_insight in acc_insights:     
            campaign_name = ''
            adset_name = ''
            ad_name = ''
            amount_spent = ''
            objective = ''
            amount_subscribe = '' 
            subscribe_conversion_value = ''
            amount_install = '' 
            amount_purchase = ''    
            purchase_conversion_value = ''
            reporting_date = ''          
            cpm = ''
            ctr = ''
            impressions = ''         

            # insights/campaign_name得到 "產品包名稱"
            if 'campaign_name' in acc_insight:
                campaign_name = acc_insight['campaign_name']
                if '⛔️股市爆料同學會' in campaign_name:
                    campaign_name = campaign_name.replace('⛔️', '')

            # insights/adset_name得到 "產品名稱"
            if 'adset_name' in acc_insight:
                adset_name = acc_insight['adset_name']
                
            # insights/adset_name得到 "廣告名稱"
            if 'ad_name' in acc_insight:
                ad_name = acc_insight['ad_name']
            
            # insights/cpm得到 "每一次廣告的曝光成本"
            if 'cpm' in acc_insight:
                cpm = acc_insight['cpm']
                
            # insights/ctr得到 "廣告點擊率"
            if 'ctr' in acc_insight:
                ctr = acc_insight['ctr']

            # insights/ctr得到 "廣告觸及人數"
            if 'impressions' in acc_insight:
                impressions = acc_insight['impressions']
                
            # insights/spend得到 "總支出成本"
            if 'spend' in acc_insight:    
                amount_spent = acc_insight['spend']

            # insights/objective得到 "廣告目標"
            if 'objective' in acc_insight:
                objective = acc_insight['objective']

            # insights/date_stop得到 "報表終止日期"
            if 'date_stop' in acc_insight:
                reporting_date = datetime.strptime(acc_insight['date_stop'], '%Y-%m-%d').strftime('%Y%m%d')

            # insights/actions得到 "軟體下載數/課程購買數"
            if 'actions' in acc_insight:
                for action in acc_insight['actions']:
                    if action['action_type'] == 'mobile_app_install': 
                        amount_install = action['value']
                    if action['action_type'] == 'purchase':
                        amount_purchase = action['value']

            # insights/action_values得到 "購買轉換價值"
            if 'action_values' in acc_insight:
                for ac_values in acc_insight['action_values']:
                    if ac_values['action_type'] == 'omni_purchase':
                        purchase_conversion_value = ac_values['value']

            # insights/conversions得到 "訂閱數"
            if 'conversions' in acc_insight:
                for cv in acc_insight['conversions']:
                    if cv['action_type'] == 'subscribe_total':
                        try:
                            amount_subscribe = cv['7d_click']
                        except:
                            amount_subscribe = cv['value']

            # insights/conversion_values得到 "訂閱轉換價值"             
            if 'conversion_values' in acc_insight:
                for cv_values in acc_insight['conversion_values']:
                    if cv_values['action_type'] == 'subscribe_total':
                        try:
                            subscribe_conversion_value = cv_values['7d_click']
                        except:
                            subscribe_conversion_value = cv_values['value']

            # 將資料以List的形式儲存
            try:
                Campaign_Name.append(campaign_name)
                Adset_Name.append(adset_name)
                Ad_Name.append(ad_name)
                Objective.append(objective)
                Amount_Spent.append(amount_spent)
                Amount_Install.append(amount_install)
                Amount_Purchase.append(amount_purchase)
                Purchase_Conversion_Value.append(purchase_conversion_value)
                Amount_Subscribe.append(amount_subscribe)
                Subscribe_Conversion_Value.append(subscribe_conversion_value)
                Reporting_Date.append(reporting_date)
                CPM.append(cpm)
                CTR.append(ctr)
                Impressions.append(impressions)
                
            except UnicodeEncodeError as e:
                print(e)

# 這裡需要調整一下，想一下演算法，讓這裡能夠完美fit
            # obj_time為目標時間
            # 當目標時間 < 結束時間時，結束時間將為目標時間 
            # if obj_time < end_time:
            #     end_time = obj_time
            #     stop_flag += 1    

            if now_time < end_time:
                end_time = now_time
                stop_flag += 1     

        print('第{}次'.format(count)) 
        # 每次爬蟲完，休息60秒
        time.sleep(60)
        
        # 當成是執行4次時，暫停時間120秒
        if count%4 == 0:
            time.sleep(120)
        
        # 將資料以Dataaframe的方式儲存
        raw_data = {'Campaign_Name': Campaign_Name,
                 'Adset_Name': Adset_Name,
                 'Ad_Name': Ad_Name,
                 'Objective': Objective,
                 'Amount_Spent': Amount_Spent,
                 'Amount_Install': Amount_Install,
                 'Amount_Purchase': Amount_Purchase,
                 'Purchase_Conversion_Value': Purchase_Conversion_Value,
                 'Amount_Subscribe': Amount_Subscribe,
                 'Subscribe_Conversion_Value': Subscribe_Conversion_Value,
                 'CPM': CPM,
                 'CTR': CTR,
                 'Impressions': Impressions,
                 'Reporting_Date': Reporting_Date,}
        df = pd.DataFrame.from_dict(raw_data)
        
        # 當開始時間和結束時間相等時，立即結束程式
        if start_time == end_time:
            break
    return df

# %% 
# 獲取SN和SD
uncleaned_data = Get_SN_RD(TableName)
print(uncleaned_data)

# 將SN和SD清洗出來
cleaned_SN, cleaned_RD = Clean_both_SN_RD(uncleaned_data)
print(cleaned_SN, cleaned_RD)

# 回朔時間
new_reporting_date = RD_date_back(cleaned_RD)
print('new_reporting_date(回溯一個月的時間): ', new_reporting_date.strftime('%Y%m%d'), end ='\n')

# 開始爬蟲的時間
start_time = new_reporting_date 
print('start_time(開始爬蟲時間): ', start_time.strftime('%Y%m%d'))

# 結束爬蟲的時間
now_time = datetime.now()
print('now_time(結束爬蟲時間):  ', now_time.strftime('%Y%m%d'))
# 一次以一個星期爬蟲
end_time = new_reporting_date + relativedelta(weeks = 1)
print('end_time(給主程式跑的一星期): ', end_time.strftime('%Y%m%d'))

# 刪除資料
DelSQL(TableName, start_time.strftime('%Y%m%d'), now_time.strftime('%Y%m%d'))

# 重新拿取SQL內的SN, RD資料
updated_uncleaned_data = Get_SN_RD(TableName)
print(updated_uncleaned_data)

# 再次清洗SN, RD資料
cleaned_SN, cleaned_RD = Clean_both_SN_RD(updated_uncleaned_data)
print(cleaned_SN, cleaned_RD)
# %%
# 抓取一個月資料，並分成一次一星期
# start_time = datetime.strptime(str(cleaned_RD), '%Y%m%d').date()
# end_time = start_time + relativedelta(weeks = 1)
# obj_time =  start_time + relativedelta(months = 1)
# print(obj_time.strftime('%Y%m%d'))

# 嘗試模組化我們的資料結構
try:
    # df = main_prog(start_time, end_time, obj_time)
    df = main_prog(start_time, end_time, now_time)
    # 將df擴增流水號和廣告種類標籤
    df = Expand_df(df, cleaned_SN)
    # 將df內的空值""，儲存成None的型態
    df = Blank_to_None(df)
    # 將df儲存成csv檔案保存
    Df_to_Csv(df)
    #將資料型態轉為字串,但實際上不轉換寫入SQL時,SQL也會自動處理
    df['Reporting_Date'] = df['Reporting_Date'].astype('str') 
    InsertSQL(df, TableName)
    Msg = '{} The program works well'.format(datetime.now().date())
    
except Exception as e:
    error_class = e.__class__.__name__ #取得錯誤類型
    detail = e.args[0] #取得詳細內容
    _, _, tb = sys.exc_info() #取得Call Stack
    lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
    fileName = lastCallStack[0] #取得發生的檔案名稱
    lineNum = lastCallStack[1] #取得發生的行號
    funcName = lastCallStack[2] #取得發生的函數名稱
    Msg = "File \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)
    print(Msg, '\n')

send_email(Msg)
# flag確立program完整結束
print('--- %s seconds ---' % (time.time() - run_time))

# %%
# %%
