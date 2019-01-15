import snowflake.connector
import boto3
from boto3 import Session

client = boto3.client('s3')
resource = boto3.resource('s3')
bucketName = "yusufqedanbucket"
bucket = resource.Bucket(bucketName)
aws_credentials = Session().get_credentials().get_frozen_credentials()

file = open("/home/yusuf/.snowflake_credentials", "r")
line = file.readline()[:-1]
output = line.split(',')

con = snowflake.connector.connect(
    user=output[0],
    password=output[1],
    account=output[2]
)

con.cursor().execute("USE FOOD_MART_AGG")

con.cursor().execute("TRUNCATE TABLE FOOD_MART_AGG.PUBLIC.SALES_AGG")

con.cursor().execute("""
COPY INTO sales_agg FROM s3://""" + bucketName + "/trg/final_csv" """
    CREDENTIALS = (
        aws_key_id='{aws_access_key_id}',
        aws_secret_key='{aws_secret_access_key}')
    FILE_FORMAT=(field_delimiter=',')
""".format(
    aws_access_key_id=aws_credentials.access_key,
    aws_secret_access_key=aws_credentials.secret_key))

con.close()
