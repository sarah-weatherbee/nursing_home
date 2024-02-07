

import boto3


# connect to s3
s3_resource = boto3.resource('s3')


import uuid
def create_bucket_name(bucket_prefix):
    # The generated bucket name must be between 3 and 63 chars long
    return ''.join([bucket_prefix, str(uuid.uuid4())])


def create_bucket(bucket_prefix, s3_connection):
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    bucket_response = s3_connection.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': current_region})
    print(bucket_name, current_region)
    return bucket_name, bucket_response

first_bucket_name, first_response = create_bucket(
    bucket_prefix='cms-nh-fac-wk', 
    s3_connection=s3_resource.meta.client)

first_response

second_bucket_name, second_response = create_bucket(
    bucket_prefix='cms-nh-county-agg', s3_connection=s3_resource)

second_response