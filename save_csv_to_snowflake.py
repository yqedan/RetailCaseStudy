import snowflake.connector
import boto3

client = boto3.client('s3')
resource = boto3.resource('s3')
bucketName = "yusufqedanbucket"
bucket = resource.Bucket(bucketName)


def read_credentials_from_s3():
    for obj in bucket.objects.all():
        if obj.key == "trg/snowflake_credentials":
            out = str(obj.get()['Body'].read())[2:-3].split(',')
            return out


output = read_credentials_from_s3()

# Gets the version
ctx = snowflake.connector.connect(
    user=output[0],
    password=output[1],
    account=output[2]
)
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()
