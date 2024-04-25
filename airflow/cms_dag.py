from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import requests
import json

def get_endpoint():
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
    return latest_distro

def extract_raw_cms_data():
  latest_data = []
  stats_response = requests.request("GET", stats_endpoint)
  stats_response = stats_response.json()
  total_rows = stats_response['total_rows']
#print(f"total rows: {total_rows}")

  date_today = date.today()
  end_wk_end_date = date_today - relativedelta(weeks=1, weekday=SU)
  start_wk_end_date = end_wk_end_date - relativedelta(weeks=3, weekday=SU)
  week = timedelta(days=7)
  offset = 0
  size = 5000

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
          # json_latest_data = latest_data.json()
          # save as parquet file
          # pq_latest_data = df_latest_data.to_parquet("data_pre_proc/nh_pre_proc_raw.parquet", engine='auto', compression='snappy', index=None, partition_cols=None)
  return df_latest_data



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 22),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}



with DAG('cms_dag',
        default_args = default_args,
        schedule_interval = '@daily',
        catchup = False) as dag:

        is_cmsnh_api_ready = HttpSensor(
                task_id = 'is_cmsnh_api_ready',
                http_conn_id = 'cmsnh_api',
                endpoint = get_endpoint()
                )

        extract_raw_cms_data = PythonOperator(
                task_id= 'extract_raw_cms_data',
                python_callable=extract_raw_cms_data
                )

        # extract_cms_data = SimpleHttpOperator(
        # task_id = 'extract_cms_data',
        # http_conn_id = 'cmsnh_api',
        # endpoint = latest_distro,
        # method = 'GET',
        # response_filter = lambda r: json.loads(r.text),
        # log_response = True
        # )

        # is_cmsnh_api_ready >> extract_raw_cms_data

