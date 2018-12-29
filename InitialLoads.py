import boto3
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

s3 = boto3.client('s3')
bucketName = "yusufqedanbucket"

url = "jdbc:mysql://localhost:3306/food_mart"
driver = "com.mysql.jdbc.Driver"
user = "root"
password = "root"

salesAllTable = "food_mart.sales_fact_all"
promotionsTable = "food_mart.promotion"
timeByDayTable = "food_mart.time_by_day"
storeTable = "food_mart.store"

# read in tables from mysql database
salesAllDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=salesAllTable, user=user, password=password).load()
promotionsDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=promotionsTable, user=user, password=password).load()
timeDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=timeByDayTable, user=user, password=password).load()
storeDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=storeTable, user=user, password=password).load()

# grab last update value for saving
lastUpdate = salesAllDf.select(max("last_update").alias("last_update"))
lastUpdate = lastUpdate.select(lastUpdate.last_update.cast(IntegerType())).collect()[0].asDict().get("last_update")

# save the last update file to s3
lastUpdateTempFile = tempfile.NamedTemporaryFile()
lastUpdateFile = open(lastUpdateTempFile.name, 'w')
lastUpdateFile.write(str(lastUpdate))
lastUpdateFile.close()
s3.put_object(Bucket=bucketName, Key="trg/last_update", Body=open(lastUpdateTempFile.name, 'rb'))
lastUpdateFile.close()


# a function we will call for each avro directory we save to s3
def write_avro_to_s3(sub_dir_name, data_frame):
    path = os.path.join(tempfile.mkdtemp(), sub_dir_name)
    data_frame.write.format("com.databricks.spark.avro").save(path)
    index = 0
    for f in os.listdir(path):
        if f.startswith('part'):
            s3.put_object(Bucket=bucketName, Key="trg/" + sub_dir_name + "/part" + str(index), Body=open(path + "/" + f, 'rb'))
            index += 1


# save the avro files
write_avro_to_s3("sales_avro", salesAllDf)
write_avro_to_s3("promotions_avro", promotionsDf)
write_avro_to_s3("timeByDay_avro", timeDf)
write_avro_to_s3("store_avro", storeDf)
