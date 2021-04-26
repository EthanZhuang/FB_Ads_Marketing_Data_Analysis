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
DBdata = {'ip':'192.168.99.142', 'db':'CMAPP', 'user142': 'cmapp', 'pwd142': '0000'}
conn_DBdata = pyodbc.connect('DRIVER={SQL Server};'\
                            'SERVER=' + DBdata['ip'] + \
                            ';DATABASE=' + DBdata['db'] +\
                            ';UID=' + DBdata['user142'] +\
                            ';PWD=' + DBdata['pwd142'] + ';')

DBdata_cr = conn_DBdata.cursor()
TableName = 'FB_AdsTest'

# %%
Act_list = {'StockAiPro_APP': '783539402272443',
'籌碼K線(原本測試帳號)': '132799196821088',
'StockAiPro_APP': '1143824496065734',
'icheck_APP': '936847127084019',
'icheck_APP': '4196385753706403',
'丹尼爾_PC+APP': '452504569395856',
'丹尼爾_PC+APP': '3871892012868660',
'價值K線_APP': '795937830999375',
'價值K線_APP': '294383082255305',
'優惠王-costco同學會_APP': '712103039460113',
'優惠王-costco同學會_APP': '467704917976698',
            
'大大PM': '509426729992360',
'大股東_APP': '753062955394894',
'大股東_APP': '1864362753722473',
'我的房價漲多少_APP': '149018927107258',
'我的房價漲多少_APP': '454759162500759',
'房屋價值地圖_APP': '157768706197434',
'房屋價值地圖_APP': '785822512339192',
'投資小學堂_APP': '219944153210593',
'投資小學堂_APP': '285019616541316',
'新聞爆料同學會_APP': '129225645812364',
            
'新聞爆料同學會_APP': '462416488363153', 
'月老_APP': '505050013831773',
'月老_APP': '462146505235011',
'期權先生_PC+APP': '197791738405867',
'期期權先生_PC+APP': '449681076323070',
'期貨電子盤_APP': '3873072282807287',
'期貨電子盤_APP': '170149104947622',
'林恩如_PC+APP': '534983857479217',
'林恩如_PC+APP': '491297538573013',
'權證小哥_PC+APP': '1132572753851821',
            
'權證小哥_PC+APP': '436996980921253',
'武財神_APP': '428844138551031',
'武財神_APP': '298213318597607',
'每日頭條_APP': '222102939378962',
'每日頭條_APP': '551649459143632',
'無聊詹_PC+APP': '440198387247896',
'無聊詹_PC+APP': '1339779816406536',
'租屋雷達_APP': '1086617258501383',
'租屋雷達_APP': '1061710770977404',
'算利教官_APP': '480153816358865',
            
'算利教官_APP': '4284601198239102',
'籌碼K線_APP': '372330733913901',
'籌碼K線_APP': '136443351648696',
'籌碼K線_PC': '3648034708655701',
'美股夢想家_PC+APP': '279643700237508',
'美股夢想家_PC+APP': '478730439945199',
'股人阿勳_APP': '1614448442086305',
'股人阿勳_APP': '830300951168633',
'股市Podcast_APP': '448233149813034',
'股市Podcast_APP': '1329909827384466',
            
'股市爆料同學會_APP': '238268321010982',
'股市爆料同學會_APP': '1757870844381494',
'自由人_PC+APP': '137512381633886',
'自由人_PC+APP': '166308061984091',
'艾蜜莉_APP': '319046682694239',
'艾蜜莉_APP': '2813564628898257',
'華倫_APP': '476405876831222',
'華倫_APP': '426164802018260',
'阿格力_APP': '2767955253458688',
'阿格力_APP': '293950655476450',
            
'阿水_PC+APP': '787416052182106',
'阿水_PC+APP': '278212370485576',
'陳重銘_APP': '268527648070343',
'陳重銘_APP': '458094148869336',
}

# %% 
# 紀錄程式跑多久時間
run_time = time.time()

# 提取流水號、報告日期
def Query_Recent_SN_RD(TableName):
    Query_Recent_SN_RD = """
        SELECT MAX([Serial_Number]), MAX([Reporting_Date])
        FROM [CMAPP].[dbo].[{}]
        """
    print(Query_Recent_SN_RD.format(TableName, TableName))
    DBdata_cr.execute(Query_Recent_SN_RD.format(TableName, TableName))
    
    # 獲取 SQL 表上最近的日期(RD)和流水號(SN)
    Last_SN, Last_RD = DBdata_cr.fetchall()[0]
    print(Last_SN, Last_RD, end = '\n\n')
    
    df = pd.DataFrame(DBdata_cr.fetchall())
#     print(DBdata_cr.description, end = '\n\n')
#     df.columns = [ x for x in DBdata_cr.description ]
    conn_DBdata.commit()
    return Last_SN, Last_RD, df

Last_SN, Last_RD, df = Query_Recent_SN_RD(TableName)
df
# %%
# 將Reporting_Date回溯1個月前
def Set_for_start_date(Last_RD):
    # 將str變成時間格式(datetime)，才能做計算
    reporting_date = datetime.strptime(str(Last_RD), '%Y%m%d')
    
    # 最近的日期(Last_RD) > 現在回推一個月後: 限制只能抓一個月內
    if reporting_date.date() < (datetime.now().date() - relativedelta(months = 1)):
        reporting_date = reporting_date.date()
    else:
        # 將日期回溯 1個月
        reporting_date = reporting_date.date() - relativedelta(months = 1)
    return reporting_date

reporting_date = Set_for_start_date(Last_RD)
## reporting date will be the start_date of the function of DelSQL
start_time = reporting_date
start_time

# %%
# 主程式內的 time_range's parameter 時間調整函數
def end_judge(start_time, end_time, obj_time):
    """
    start_time: 每一次執行程式的「開始時間」
    end_time: 每一次執行程式的「結束時間」
    obj_time: 每個任務的的「最後終止時間」
    stop_flag: 告訴 main_prog 停止程式，並建立 dataframe
    """
    # 程式碼的判斷沒有先後順序
    # 判定 end_time 是否會超過 obj_time
    if end_time >= obj_time:
        end_time = obj_time
        
    # 判定 start_time 是否會超過 end_time
    if start_time >= end_time:
        start_time = end_time 
        
    return start_time, end_time
# %%
def time_range_shift(start_time, end_time, obj_time):
    start_time += relativedelta(weeks = 1)
    end_time = start_time + relativedelta(weeks = 1)
    obj_time = obj_time
    
    # 判定時間問題
    start_time, end_time = end_judge(start_time, end_time, obj_time)
    return start_time, end_time
# %%
def Expand_df(df, Last_SN): 
    # 第幾個 col 插入流水號(SN)、廣告分類(CA)、 
    idx_SN = 0
    idx_CA = 15
    Serial_Number = []
    # 廣告類別
    Category = 'FB'

    """
    # 如果有此帳號的資料，則往前回推一個月(自動)
    # 沒有此帳號的資料，則手動輸入日期(手動)
    """
    # 再以有的資料庫下，增加新的Serial_Number & Catergory(FB, GOOGL, AAPL)
    for order in range(len(df)):
        s_num = str(df.Reporting_Date[order]) + '000' + str(int(str(Last_SN)[8:]) + order + 1)
#         print(s_num)
        Serial_Number.append(int(s_num))
    
#     for order in range(len(df)):
#         s_num = str(df['Reporting_Date'][order]) + '000' + str(order)
#         Serial_Number.append(int(s_num))
    
    
    # 將資料插入
    df.insert(loc = idx_SN, column = 'Serial_Number', value = Serial_Number)
    df.insert(loc = idx_CA, column = 'Category', value = Category)
    return df

# 將CSV內檔案的
def Blank_to_None(df):
    for col in df.columns:
        df[col] = df[col].apply(lambda x: None if x == '' else x)
    return df 

# # 將df的資料備份下來
# def Df_to_Csv(df):
#     # 將df內的Data儲存成CSV file
#     df.to_csv('{}_FBAdsTest.csv'.format(datetime.now().strftime('%Y%m%d')), index = False, encoding='utf_8_sig')
#     return None

# # 刪除資料
# def DelSQL(TableName: str, start_time, now_time):
#     str_query = """
#         DELETE
#         FROM [CMAPP].[dbo].[{}]
#         WHERE Reporting_Date BETWEEN {} AND {}
#         """
#     print(str_query.format(TableName, start_time, now_time))
#     DBdata_cr.execute(str_query.format(TableName, start_time, now_time))
#     conn_DBdata.commit()   

# 寫入資料
def InsertSQL(df, TableName):
    str_query = """
        INSERT INTO [CMAPP].[dbo].[{}]([Serial_Number],[Campaign_Name],[Adset_Name],[Ad_Name],[Objective],[Amount_Spent],[Amount_Install],\
        [Amount_Purchase],[Purchase_Conversion_Value],[Amount_Subscribe],[Subscribe_Conversion_Value],[CPM],[CTR],[Impressions],[Reporting_Date]\
        ,[Category],[Product_Name],[Act_ID],[Attribution])VALUES({})
        """
    # 將csv DataFrame每筆資料變成 list 型態
    insert_data = df.values.tolist()
    for i in range(len(insert_data)):
#         print(str_query.format(TableName, str(insert_data[i]).strip('[]').replace('None', 'null')))      
        DBdata_cr.execute(str_query.format(TableName, str(insert_data[i]).strip('[]').replace('None', 'null')))
        conn_DBdata.commit() 

def send_email(Text):
    socket.getaddrinfo('127.0.0.1', 8080)

    MailSender = 'ai@cmoney.tw'
    MailSenderPwd = '1qaz@wsx'
    MailReceivers = ['love824671@gmail.com','sam_liao@comney.com.tw', 'cathy_want@comney.com.tw']
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
def main_prog(act_id: int, product_name: str, start_time, end_time, obj_time):
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
    ad_account_id = 'act_{}'.format(act_id)
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
    Act_ID = list()
    Product_Name = list()
    Attribution = list()

    # 紀錄程式跑多久時間
    run_time = time.time()

    # 比較時間前後順序
    while True:
        # 計算迴圈跑幾次
        count += 1

        # 了解時間怎麼走
        print(start_time.strftime('%Y-%m-%d'))
        print(end_time.strftime('%Y-%m-%d'))

        # Insight API中的fields資料(控制日期條件、資料的層級)
        # start_time和end_time需要為string(%Y-%m-%d)
        params = {
            'time_range': {
                        'since': start_time.strftime('%Y-%m-%d'),\
                        'until': end_time.strftime('%Y-%m-%d'),
            }, 
            # level從campaign改成adset
            'level': {'ad'},
            'time_increment': '1',
            # 手動設定的歸因: 1天點擊/讀取後(和廣告預設值有落差)
            'action_attribution_windows': {'1d_click', '1d_view'},
        }

        # 廣告帳號獲取廣告Insights資訊
        acc_insights = AdAccount(ad_account_id).get_insights(
            fields = fields,
            params = params,
        )

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
            attribution = '1d_Click/View'

            # insights/campaign_name得到 "廣告包名稱"
            if 'campaign_name' in acc_insight:
                campaign_name = acc_insight['campaign_name']
                if '⛔️股市爆料同學會' in campaign_name:
                    campaign_name = campaign_name.replace('⛔️', '')

            # insights/adset_name得到 "廣告set名稱"
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
                Act_ID.append(act_id)
                Product_Name.append(product_name)
                Attribution.append(attribution)
            except UnicodeEncodeError as e:
                print(e)
            
#             print(acc_insight['campaign_name'], '\n', acc_insight['adset_name'], '\n', acc_insight['ad_name'], '\n', acc_insight)

        print('第{}次'.format(count)) 
        # 如果是籌碼K線原本的帳號，停比較多時間
        if act_id == '132799196821088':
            time.sleep(180)
            if count%2 == 0:
                time.sleep(120)
                
        # 當成是執行4次時，暫停時間300秒
        if count%3 == 0:
            time.sleep(10)

        
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
                 'Reporting_Date': Reporting_Date,
                 'Product_Name': Product_Name,
                 'Act_ID': Act_ID,
                 'Attribution': Attribution,}   
        df = pd.DataFrame.from_dict(raw_data)
        
        # 時間控制應該ㄗㄞ
        start_time, end_time = end_judge(start_time, end_time, obj_time)
        start_time, end_time = time_range_shift(start_time, end_time, obj_time)
#         print('調整後，下次程式的時間', start_time.strftime('%Y-%m-%d'))
#         print('調整後，下次程式的時間', end_time.strftime('%Y-%m-%d'))

        # 當開始時間和結束時間相等時，立即結束程式
        if start_time == end_time:
            break
            
    # flag確立program完整結束
    print('--- %s seconds ---' % (time.time() - run_time))
    
    # 如果有蒐集到任何數據，any(raw_data) == True
    if any(raw_data.values()):
        print('有acc_insight資料') 
        insight_flag = 1 
    else:
        print('沒東西')
        insight_flag = 0
    return df, insight_flag

# %% 

# 確認一下廣告到底有沒有打進去(這要放外面，乾你娘)
act_get_acc_insight = dict() 
df_origin = pd.DataFrame()
count_for_df_combo = 0

suspend_sign = 0
try: 
    for product_name, act_id in Act_list.items():
        
        # 起始的時間資料
#         start_time = datetime(2020, 1, 26).date()
        start_time = reporting_date + relativedelta(days = 1)
        end_time = start_time + relativedelta(weeks = 1)
#         obj_time = datetime.now().date()
#         obj_time = datetime(2020, 3, 2).date()
        obj_time = start_time + relativedelta(weeks = 1)
    
        # df will refresh every time, since there are many Ads-accounts to work with
        df, insight_flag = main_prog(act_id, product_name, start_time, end_time, obj_time)
        
        #### 這裡可以寫成一個 function

        # 確認哪些 act_id 是已經有廣告資料
        if insight_flag == 1:
            act_get_acc_insight[act_id] = 1
            
            if df_origin.empty & count_for_df_combo == 0:
            # 合併 df_accu & df，
#                 df_combo = df_accu.append(df)
                df_combo = pd.concat([df_origin, df], ignore_index = True) # ignore index
#                 suspend_sign += 1
                count_for_df_combo += 1
            else: 
                df_combo = pd.concat([df_combo, df], ignore_index = True) # ignore index
#             print('suspend_sign: ', suspend_sign)
            print(df)
    
        else: 
            act_get_acc_insight[act_id] = 0
            print(df)
        
        # 程式執行 ? 次就停止
        if suspend_sign == 3:
            break
    sucMsg = 'Working well'
    
except Exception as e:
    error_class = e.__class__.__name__ #取得錯誤類型
    detail = e.args[0] #取得詳細內容
    _, _, tb = sys.exc_info() #取得Call Stack
    lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
    fileName = lastCallStack[0] #取得發生的檔案名稱
    lineNum = lastCallStack[1] #取得發生的行號
    funcName = lastCallStack[2] #取得發生的函數名稱
    errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)
    print(errMsg, '\n')

# %%
# 一般 DF 讀取
df_combo = Expand_df(df_combo, Last_SN)
# %%
# 一般 DF 讀取
Blank_to_None(df_combo)
# 一般 DF 
df_combo['Reporting_Date'] = df_combo['Reporting_Date'].astype('str')
df_combo['Act_ID'] = pd.to_numeric(df_combo['Act_ID'])
InsertSQL(df_combo, TableName)