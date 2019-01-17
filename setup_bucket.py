import boto3


def get_boto3_connection():
    client = boto3.client('s3')
    resource = boto3.resource('s3')
    bucket_name = "yusufqedanbucket"
    return client, resource, bucket_name
