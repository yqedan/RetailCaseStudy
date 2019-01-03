import boto3
import os
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.38,org.apache.spark:spark-avro_2.11:2.4.0 /mnt/c/Users/Jake\ Barone/PycharmProjects/RetailCaseStudy/AVRO_Parquet.py

client = boto3.client('s3')
resource = boto3.resource('s3')
bucketName = "baronejake"
bucket = resource.Bucket(bucketName)

spark = SparkSession.builder \
 .master("local") \
 .appName("AVRO_to_joined_parquet") \
 .getOrCreate()
spark.sparkContext.setLogLevel('WARN')


def get_avro_from_s3(sub_dir_name):
    # see if we have a last update file in s3
    df = None
    for obj in bucket.objects.all():
        key = obj.key
        if key.startswith("trg/" + sub_dir_name + "/init") or key.startswith("trg/" + sub_dir_name + "/update"):
            file = tempfile.NamedTemporaryFile(delete=False)
            file.write(obj.get()['Body'].read())
            file.close()
            df_new = spark.read.format("com.databricks.spark.avro").load(file.name)
            if df is None:
                df = df_new
            else:
                df.union(df_new)
    return df


# get the avro files from s3
promotionDF = get_avro_from_s3("promotions_avro")
salesDF = get_avro_from_s3("sales_avro")
timeDF = get_avro_from_s3("timeByDay_avro")
storeDF = get_avro_from_s3("store_avro")

# joins
SaSt_DF = salesDF.join(storeDF, "store_id")
STS_DF = SaSt_DF.join(timeDF, "time_id")
joinedDF = promotionDF.join(STS_DF, "promotion_id")

# select only needed columns
finalDF = joinedDF.select(promotionDF.promotion_id.cast(IntegerType()),
                          storeDF.region_id.cast(IntegerType()),
                          timeDF.the_year.cast(IntegerType()),
                          timeDF.the_month,
                          timeDF.the_day,
                          salesDF.last_update.cast(IntegerType()),
                          promotionDF.cost.cast(FloatType()),
                          salesDF.store_sales.cast(FloatType()))

# save new parquet to temp directory
path = os.path.join(tempfile.mkdtemp(), "joined_parquet")
finalDF.filter(promotionDF.promotion_id != "0").repartition(1).write.save(path)

# select and save just the parquet file to s3 which is always the 3rd position in the directory
parquet_file = os.listdir(path)[3]
client.put_object(Bucket=bucketName, Key="trg/joined_parquet", Body=open(path + "/" + parquet_file, 'rb'))

