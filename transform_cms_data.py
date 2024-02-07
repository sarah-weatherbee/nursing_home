import pandas as pd

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import table
from urllib.request import urlopen
import boto3


def transform_cms_data(cdf_latest_data_pq):
    
    ti = kwargs['ti']
    s3 = s3fs.S3FileSystem()

    fips_codes_df = pq.ParquetDataset(
        's3://fips-codes-smw/fips-codes/data.parquet',
        filesystem=s3,
    ).read_pandas().to_pandas()

    fips_codes_df.head()
    
    cdf_latest_data_pq = ti.xcom_pull(task_ids="extract_cms_data")
    transformed_cms_data = cdf_latest_data_pq
    return transformed_cms_data