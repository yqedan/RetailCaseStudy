import sys
import boto3
import os
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.38,com.databricks:spark-avro_2.11:4.0.0 IncrementalLoads.py

client = boto3.client('s3')
resource = boto3.resource('s3')
bucketName = "yusufqedanbucket"
bucket = resource.Bucket(bucketName)

url = "jdbc:mysql://localhost:3306/food_mart"
driver = "com.mysql.jdbc.Driver"
user = "root"
password = "root"

salesAllTable = "food_mart.sales_fact_all"

# see if we have a last update file in s3
lastUpdate = 0
found = False
for obj in bucket.objects.all():
    key = obj.key
    if key == "trg/last_update":
        lastUpdate = int(obj.get()['Body'].read())
        found = True
        break
if found is False:
    print("Error: can\'t find last update file maybe run an initial load first?")
    sys.exit()

spark = SparkSession.builder \
 .master("local") \
 .appName("Incremental_Loads_For_Retail_Agg") \
 .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# read in table from mysql database
salesAllDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=salesAllTable, user=user, password=password).load()
# select all but cast date column timestamp to integer for filter logic
salesAllDf = salesAllDf.select("product_id", "time_id", "customer_id", "promotion_id", "store_id", "store_sales", "store_cost", "unit_sales", col("last_update").cast("integer"))
# grab only newest records
salesAllDfLatest = salesAllDf.filter(salesAllDf.last_update > lastUpdate)
if salesAllDfLatest.count() > 0:
    # grab last update value for saving
    lastUpdateNewRow = salesAllDfLatest.select(max("last_update").alias("last_update"))
    lastUpdateNew = lastUpdateNewRow.select(lastUpdateNewRow.last_update).collect()[0].asDict().get("last_update")
    # save the last update file to s3
    lastUpdateTempFile = tempfile.NamedTemporaryFile()
    lastUpdateFile = open(lastUpdateTempFile.name, 'w')
    lastUpdateFile.write(str(lastUpdateNew))
    lastUpdateFile.close()
    client.put_object(Bucket=bucketName, Key="trg/last_update", Body=open(lastUpdateTempFile.name, 'rb'))
    lastUpdateFile.close()
    # save sales avro to s3
    path = os.path.join(tempfile.mkdtemp(), "sales_avro")
    salesAllDfLatest.write.format("com.databricks.spark.avro").save(path)
    index = 0
    for f in os.listdir(path):
        if f.startswith('part'):
            client.put_object(Bucket=bucketName, Key="trg/sales_avro/update_" + str(lastUpdateNew) + "_part" + str(index), Body=open(path + "/" + f, 'rb'))
            index += 1
else:
    print("No new rows found...Aborting save!")
