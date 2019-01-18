import setup_bucket
import os
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.38,org.apache.spark:spark-avro_2.11:2.4.0 InitialLoads.py

spark = SparkSession.builder \
 .master("local") \
 .appName("Initial_Loads_For_Retail_Agg") \
 .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

conn = setup_bucket.get_boto3_connection()
client = conn[0]
resource = conn[1]
bucketName = conn[2]
bucket = resource.Bucket(bucketName)

url = "jdbc:mysql://localhost:3306/food_mart"
driver = "com.mysql.jdbc.Driver"
user = "root"
password = "root"

salesAllTable = "food_mart.sales_fact_all"
promotionsTable = "food_mart.promotion"
timeByDayTable = "food_mart.time_by_day"
storeTable = "food_mart.store"

# delete old data from s3
for obj in bucket.objects.all():
    key = obj.key
    keyTokens = key.split("/")
    if keyTokens[0] == "trg":
        obj.delete()

# read in tables from mysql database
salesAllDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=salesAllTable, user=user, password=password).load()
promotionsDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=promotionsTable, user=user, password=password).load()
timeDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=timeByDayTable, user=user, password=password).load()
storeDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=storeTable, user=user, password=password).load()


# a function we will call for each last update file we save to s3
def write_last_update_to_s3(sub_dir_name, data_frame):
    # grab last update value for saving
    last_update = data_frame.select(max("last_update").alias("last_update"))
    last_update = last_update.select(last_update.last_update.cast(IntegerType())).collect()[0].asDict().get("last_update")
    # save the last update file to s3
    last_update_temp_file = tempfile.NamedTemporaryFile()
    last_update_file = open(last_update_temp_file.name, 'w')
    last_update_file.write(str(last_update))
    # we have to close and reopen this file as binary
    last_update_file.close()
    client.put_object(Bucket=bucketName, Key="trg/" + sub_dir_name + "/last_update", Body=open(last_update_temp_file.name, 'rb'))
    last_update_file.close()


# save the last update files
write_last_update_to_s3("sales_avro", salesAllDf)
write_last_update_to_s3("promotions_avro", promotionsDf)


# a function we will call for each avro directory we save to s3
def write_avro_to_s3(sub_dir_name, data_frame):
    path = os.path.join(tempfile.mkdtemp(), sub_dir_name)
    data_frame.write.format("avro").save(path)
    index = 0
    for f in os.listdir(path):
        if f.startswith('part'):
            client.put_object(Bucket=bucketName, Key="trg/" + sub_dir_name + "/init_part" + str(index), Body=open(path + "/" + f, 'rb'))
            index += 1


# save the avro files
write_avro_to_s3("sales_avro", salesAllDf)
write_avro_to_s3("promotions_avro", promotionsDf)
write_avro_to_s3("timeByDay_avro", timeDf)
write_avro_to_s3("store_avro", storeDf)
