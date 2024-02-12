import pandas as pd

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import table
from urllib.request import urlopen
import boto3
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
import json

def transform_cms_data(ti):
    
    
    s3 = s3fs.S3FileSystem()

    fips_codes_df = pq.ParquetDataset(
        's3://fips-codes-smw/fips-codes/data.parquet',
        filesystem=s3,
    ).read_pandas().to_pandas()
    fips_codes_df.head()
    
    raw_cms_data = ti.xcom_pull(task_ids="extract_cms_data")
    
    
    transformed_cms_data = raw_cms_data
    
    return transformed_cms_data