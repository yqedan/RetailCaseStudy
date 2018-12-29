import boto3
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
# save to new directories
salesAllDf.write.format("com.databricks.spark.avro").mode("overwrite").save("/home/Yusuf/trg/sales_avro")
promotionsDf.write.format("com.databricks.spark.avro").mode("overwrite").save("/home/Yusuf/trg/promotions_avro")
timeDf.write.format("com.databricks.spark.avro").mode("overwrite").save("/home/Yusuf/trg/timeByDay_avro")
storeDf.write.format("com.databricks.spark.avro").mode("overwrite").save("/home/Yusuf/trg/store_avro")
# grab last update value for saving
lastUpdate = salesAllDf.select(max("last_update").alias("last_update"))
lastUpdate = lastUpdate.select(lastUpdate.last_update.cast(IntegerType())).collect()[0].asDict().get("last_update")
# save the new file with value
lastUpdateFile = open("/home/Yusuf/trg/last_update", "w")
lastUpdateFile.seek(0)
lastUpdateFile.write(str(lastUpdate))
lastUpdateFile.close()

s3.put_object(Bucket=bucketName, Key="trg/last_update", Body=open("/home/Yusuf/trg/last_update", 'rb'))

# s3.put_object(Bucket=bucketName, Key="trg/sales_avro", Body=open("/home/Yusuf/trg/sales_avro", 'rb'))
# s3.put_object(Bucket=bucketName, Key="trg/promotions_avro", Body=open("/home/Yusuf/trg/promotions_avro", 'rb'))
# s3.put_object(Bucket=bucketName, Key="trg/timeByDay_avro", Body=open("/home/Yusuf/trg/timeByDay_avro", 'rb'))
# s3.put_object(Bucket=bucketName, Key="trg/store_avro", Body=open("/home/Yusuf/trg/store_avro", 'rb'))


