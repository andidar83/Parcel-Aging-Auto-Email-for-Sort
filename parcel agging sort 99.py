# -*- coding: utf-8 -*-
"""
Created on Mon Sep  4 14:36:29 2023

@author: NXP
"""

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import ssl
import smtplib
import sys
import pandas as pd
import os
import requests
import time
from pprint import pprint
import json
import gspread as gs
import gspread_dataframe as gd
from pretty_html_table import build_table
from datetime import datetime, timedelta

credentials ={
}
gc = gs.service_account_from_dict(credentials)

  
def poll_job(s, redash_url, job):
    # TODO: add timeout
    while job['status'] not in (3,4):
        response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
        job = response.json()['job']
        time.sleep(1)

    if job['status'] == 3:
        return job['query_result_id']
    
    return None


def get_fresh_query_result(redash_url, query_id, api_key, params):
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})

    payload = dict(max_age=0, parameters=params)

    response = s.post('{}/api/queries/{}/results'.format(redash_url, query_id), data=json.dumps(payload))

    if response.status_code != 200:
        return 'Refresh failed'
        raise Exception('Refresh failed.')

    result_id = poll_job(s, redash_url, response.json()['job'])

    if result_id:
        response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
        if response.status_code != 200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')

    return response.json()['query_result']['data']['rows']

def export_to_sheets(file_name,sheet_name,df,mode='r'):
    ws = gc.open(file_name).worksheet(sheet_name)
    if(mode=='w'):
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=True,resize=True)
        return True
    elif(mode=='a'):
        #ws.add_rows(4)
        old = gd.get_as_dataframe(worksheet=ws)
        updated = pd.concat([old,df])
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=updated,include_index=False,include_column_header=True,resize=True)
        return True
    else:
        return gd.get_as_dataframe(worksheet=ws)

now = datetime.now()
end_date = str(now)[0:16]
start_date = str(now-timedelta(days=3))[0:11]+"00:00"
print (start_date)
print (end_date)

end1_for_raw= end_date
start1_for_raw = str(now-timedelta(days=1))[0:11]+"00:00"
end2_for_raw= str(now-timedelta(days=2))[0:11]+"23:59"
start2_for_raw = str(now-timedelta(days=3))[0:11]+"00:00"

print(start1_for_raw," ",end1_for_raw )
print(start2_for_raw," ",end2_for_raw)

while True:
    try:
        print("pullingredash_1")
        params = {}
        api_key = 'yourkey'
        result = get_fresh_query_result('https://redash-id.ninjavan.co/',2221, api_key, params) #2146 2135     
        break
    except:
        print('>> Pulling failed, retrying...')

while True:
    try:
        print("pullingredash_2")
        params1 = {}
        api_key1 = 'your key'
        result1 = get_fresh_query_result('https://redash-id.ninjavan.co/',2220, api_key1, params1) #2145 2143
        break
    except:
        print('>> Pulling failed, retrying...')





rawaging = pd.DataFrame(result1)


rawaging = rawaging[["tracking_id","origin_hub","dest_hub","aging_hour","shipment_id","shipment_status"]]
rawaging["shipment_id"] = rawaging["shipment_id"].fillna("-")
rawaging["shipment_status"] = rawaging["shipment_status"].fillna("Not yet ATS")

################################SUB JUK HYPERCARE###############################################################
import numpy as np
# Filter the DataFrame based on conditions

hypercaresub_juk = rawaging[(rawaging['origin_hub'].isin(['SUB-JUK'])) & (rawaging['dest_hub'].isin(['SUB-NJK',
'SUB-KDR',
'SUB-PRK',
'SUB-MRT',
'SUB-BLT',
'SUB-LMG',
'SUB-MAD',
'SUB-MGT',
'SUB-KRJ',
'SUB-MJK',
'SUB-BDK',
'SUB-NGW',
'SUB-PCT',
'SUB-PNG',
'SUB-TRL',
'SUB-TBN',
'SUB-TLG',
'SUB-PRO',
'SUB-BJN',
'SUB-GAS',
'SUB-JOM']))]


hypercarekds_kds = rawaging[(rawaging['origin_hub'].isin(['KDS-KDS'])) & (rawaging['dest_hub'].isin([
'KDS-JPA',
'KDS-KJW',
'KDS-BLA',
'KDS-CPU',
'KDS-PWD',
'KDS-PTI',
'KDS-TYU',
'KDS-GBS',
'KDS-RBG',
'KDS-KUD'])) ]

hypercare = pd.concat([hypercaresub_juk,hypercarekds_kds])

conditions = [(hypercare['aging_hour'] >= 0) & (hypercare['aging_hour'] <= 6),
              (hypercare['aging_hour'] > 6) & (hypercare['aging_hour'] <= 12),
              (hypercare['aging_hour'] > 12) & (hypercare['aging_hour'] <= 24),
              (hypercare['aging_hour'] > 24) & (hypercare['aging_hour'] <= 48),
              (hypercare['aging_hour'] > 48) & (hypercare['aging_hour'] <= 72),
              (hypercare['aging_hour'] > 72)]
values = ['0-6','6-12','12-24','24-48','48-72','>72']

hypercare['aging_category'] = np.select(conditions,values)


# Pivot the DataFrame
pivot_hypercare = pd.pivot_table(hypercare, 
                          index=['origin_hub', 'dest_hub', 'shipment_status'],
                          columns='aging_category',
                          values='tracking_id',
                          aggfunc='count').reset_index()

# Rename the value column
# pivot_hypercare.rename(columns={'tracking_id': 'aging >24'}, inplace=True)
# pivot_hypercare= pivot_hypercare.sort_values(by = ['aging >24'], ascending=[False])
# Display the pivoted DataFrame
pivot_hypercare.columns.name = None

# Fill NaN values with '0'
pivot_hypercare.fillna(0, inplace=True)
# Display the pivoted DataFrame
# Identify numeric columns
numeric_columns = pivot_hypercare.select_dtypes(include='number').columns

# Convert numeric columns to integers


# Reset the index
pivot_hypercare.reset_index(drop=True, inplace=True)

# Remove the name from the column index
pivot_hypercare.columns.name = None


#REORDER COLUMNS
# List of columns in the desired order
desired_columns = ['origin_hub', 'dest_hub', 'shipment_status', '0-6', '6-12', '12-24', '24-48', '48-72', '>72']

# Filter out columns that exist in the DataFrame
existing_columns = [col for col in desired_columns if col in pivot_hypercare.columns]


# Reorder the columns
pivot_hypercare = pivot_hypercare[existing_columns]




# Reorder the DataFrame columns according to the desired order
pivot_hypercare = pivot_hypercare.reindex(columns=desired_columns)
pivot_hypercare.fillna(0, inplace=True)
pivot_hypercare[numeric_columns] = pivot_hypercare[numeric_columns].astype(int)
pivot_hypercare['Grand Total'] = pivot_hypercare[['0-6', '6-12', '12-24', '24-48', '48-72', '>72']].sum(axis=1)
pivot_hypercare = pivot_hypercare.sort_values(by=['origin_hub','Grand Total','shipment_status'], ascending=False)


pivot_hypercare.to_excel("revised.xlsx")
print(pivot_hypercare)




# print(pivot_hypercare[['aging_hour','aging_category']])

# rawaging = rawaging[(rawaging['aging_hour']>24)]

##################################################################################################################


# redashdata1 = pd.read_excel("remainingiv.xlsx")
# rawaging = pd.read_excel("raw_remainingiv.xlsx")
redashdata1 = pd.DataFrame(result)
data_1_all = redashdata1.drop(['total_orders_TikTok'],axis=1)
data_1_TT = redashdata1.drop(['total_orders'],axis=1)

pivot_df_all = data_1_all.pivot(index='origin_hub', columns='aging_hour', values=['total_orders']).reset_index()
pivot_df_tt = data_1_TT.pivot(index='origin_hub', columns='aging_hour', values=['total_orders_TikTok']).reset_index()

pivot_df_all.columns = pivot_df_all.columns.get_level_values(1)
pivot_df_all.rename(columns={"":"origin_hub"},inplace=True)
pivot_df_tt.columns = pivot_df_tt.columns.get_level_values(1)
pivot_df_tt.rename(columns={"":"origin_hub"},inplace=True)
pivot_df_all.columns.name = None
pivot_df_tt.columns.name = None

pivot_df_all[["0-6","6-12","12-24","24-48","48-72",">72"]] = pivot_df_all[["0-6","6-12","12-24","24-48","48-72",">72"]].fillna(0).astype(int)
pivot_df_tt[["0-6","6-12","12-24","24-48","48-72",">72"]] = pivot_df_tt[["0-6","6-12","12-24","24-48","48-72",">72"]].fillna(0).astype(int)
pivot_df_all = pivot_df_all[["origin_hub","0-6","6-12","12-24","24-48","48-72",">72"]]
pivot_df_tt = pivot_df_tt[["origin_hub","0-6","6-12","12-24","24-48","48-72",">72"]]
print(pivot_df_all)
print(pivot_df_tt)

topoffender_all = pivot_df_all.sort_values(by = ['>72', '48-72'], ascending=[False,False]).head(5)
topoffender_tt = pivot_df_tt.sort_values(by = ['>72', '48-72'], ascending=[False,False]).head(5)
print(topoffender_all)
print(topoffender_tt)
msg = MIMEMultipart()
msg['Subject'] = "Parcel Aging Daily Report "+str(datetime.now())[:10]
msg['From'] = "Andi Darmawan"
msg['To'] = "id-ops-sort-mgmt@ninjavan.co"

html = """\
<html>
  <head></head>
  <body>
      <p>Dear all,</p>

<p>Berikut adalah report parcel aging untuk tanggal {0} </p>
<p>Beberapa hal yang menjadi fokus dalam report ini diantaranya :</p>
    
    <p>- Jumlah parcel aging per origin JUK & KDS.</p>
    <p>- Jumlah parcel aging per origin MSH.</p>
    <p>- Top offender MSH dengan aging >24 jam</p>
    
    <div style="display: flex;">
        <div style="padding: 20px;">
        <p><b>Origin JUK & KDS Hypercare</b></p>
        {1}
        </div>
    </div>
   
    <div style="display: flex;">
        <div style="padding: 20px;">
        <p><b>All Parcels</b></p>
        {2}
        </div>
        <div style="padding: 20px;">
        <p><b>TikTok Parcels</b></p>
        {3}
        </div>
    </div>
    
    <div style="display: flex;">
        <div style="padding: 20px;">
        <p><b>Top offender all parcels</b></p>
        {4}
        </div>
        <div style="padding: 20px;">
        <p><b>Top offender TikTok parcels</b></p>
        {5}
        </div>
    </div>
    
    
    <p><b>Top offender tracking_id raw data</b></p>
    <a href="https://docs.google.com/spreadsheets/d/1dZh5o_rqRrWpGBxPzPzzzsIVS58qQp1q46Vzd09UBMU/edit#gid=0">Raw Data</a>
    <p>Regards,</p> 
    <p><b>The contents of this email (including any attachments) are confidential and privileged and only intended for the recipient(s) addressed above.</b></p> 
    <p><b>If you received this email by error, please notify the sender immediately and destroy it (and all attachments) without reading, storing and/or disseminating any of its contents (in any form) to any person.</b></p> 
  </body>
</html>
""".format(str(datetime.now())[:10],build_table(pivot_hypercare,'blue_dark',text_align='center'),build_table(pivot_df_all,'blue_dark',text_align='center'),build_table(pivot_df_tt,'blue_dark',text_align='center'),build_table(topoffender_all,'blue_dark',text_align='center'),build_table(topoffender_tt,'blue_dark',text_align='center'))

# .format(build_table(datapivot, 'blue_light'))

part1 = MIMEText(html, 'html')
msg.attach(part1)

  

context = ssl.create_default_context()
with smtplib.SMTP('smtp.gmail.com', 587) as server:
    server.ehlo() 
    server.starttls(context=context)
    server.ehlo() 
    server.login('YOUR EMAIL', 'YOUR APP PASSWORD')
    server.sendmail('andi.darmawan@ninjavan.co',['andi.darmawan@ninjavan.co','id-ops-sort-mgmt@ninjavan.co','id-ops-region-heads@ninjavan.co','id-ops-sort-msh@ninjavan.co','david.noviardi@ninjavan.co','haritsa.wardani@ninjavan.co','griya.lalita@ninjavan.co','geraldine.low@ninjavan.co','wilang.perdana@ninjavan.co','nadiyah.annafatin@ninjavan.co','alya.khairunisa@ninjavan.co','ian.pratama@ninjavan.co','kania.krisnanto@ninjavan.co','zainuri.zainuri1@ninjavan.co'], msg.as_string()) 



print("Status : Email Sent")

while True:
    try:
        print("injecting raw_data")
        OCPU_inject = export_to_sheets("Parcel Aging",'Sheet1',rawaging,mode='w')
        print('>>Inject Sucess')
        break
    except:
        print('>>inject failed,retrying..')
