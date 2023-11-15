import pandas as pd
import numpy as np
import requests
import json
from pandas.tseries.offsets import Week
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta, MO, SU
import os
import time
import pyarrow

url = "https://data.cms.gov/data.json"
title = "COVID-19 Nursing Home Data"


response = requests.request("GET",url)
if response.ok:
    response = response.json()
    dataset = response['dataset']
for set in dataset:
    if title ==set['title']:
        for distro in set['distribution']:
            if 'format' in distro.keys() and 'description' in distro.keys():
                if distro['format'] == "API" and distro['description'] == "latest":
                    latest_distro = distro['accessURL']
                    print(f"The latest data for {title} can be found at {latest_distro} or {set['identifier']}") 

stats_endpoint = latest_distro + "/stats"

latest_data = []
stats_response = requests.request("GET", stats_endpoint)
stats_response = stats_response.json()
total_rows = stats_response['total_rows']
print(f"total rows: {total_rows}")


date_today = date.today()
end_wk_end_date = date_today - relativedelta(weeks=1, weekday=SU)
start_wk_end_date = end_wk_end_date - relativedelta(months=4, weekday=SU)
week = timedelta(days=7)
offset = 0
size = 5000

# while i < total_rows:
while start_wk_end_date <= end_wk_end_date:   
    for offset in range(0,total_rows,size):
        offset_url = f"{latest_distro}?filter[week_ending]={start_wk_end_date}&offset={offset}&size={size}"

        
        offset_response = requests.request("GET", offset_url)
        data = offset_response.json()
        print(f"Made request for {size} results at offset {offset}")
        if len(data) == 0:
                break
        latest_data.extend(data)
        
        offset = offset + size
        
        time.sleep(3)
        print("---")
        current_url = f"{latest_distro}?filter[week_ending]={start_wk_end_date}&offset={offset}&size={size}"
        print("Requesting",current_url)

    start_wk_end_date = start_wk_end_date + week

df_latest_data = pd.DataFrame(latest_data)
print(df_latest_data)
df_latest_data.to_csv("test.csv", index=False)

