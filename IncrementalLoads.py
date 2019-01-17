import sys
import setup_bucket
import os
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.38,org.apache.spark:spark-avro_2.11:2.4.0 IncrementalLoads.py

client = setup_bucket.get_boto3_connection()[0]
resource = setup_bucket.get_boto3_connection()[1]
bucketName = setup_bucket.get_boto3_connection()[2]
bucket = resource.Bucket(bucketName)

url = "jdbc:mysql://localhost:3306/food_mart"
driver = "com.mysql.jdbc.Driver"
user = "root"
password = "root"

salesAllTable = "food_mart.sales_fact_all"
promotionsTable = "food_mart.promotion"


def delete_new_data_flag():
    # delete new_data file flag so we can determine later if we got new rows or not
    for obj in bucket.objects.all():
        key = obj.key
        if key == "trg/new_data":
            obj.delete()


def get_last_update(sub_dir_name):
    # see if we have a last update file in s3
    for obj in bucket.objects.all():
        key = obj.key
        if key == "trg/" + sub_dir_name + "/last_update":
            return int(obj.get()['Body'].read())
    print("Error: can\'t find " + sub_dir_name + " last update file maybe run an initial load first?")
    sys.exit(1)


delete_new_data_flag()
salesLastUpdate = get_last_update("sales_avro")
promotionsLastUpdate = get_last_update("promotions_avro")

spark = SparkSession.builder \
 .master("local") \
 .appName("Incremental_Loads_For_Retail_Agg") \
 .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# read in tables from mysql database
salesAllDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=salesAllTable, user=user, password=password).load()
promotionsDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=promotionsTable, user=user, password=password).load()


# function to save the new rows to s3 for sales and promotions
def save_new_rows_to_s3(sub_dir_name, data_frame, last_update):
    # cast date last update column timestamp to integer for filter logic
    data_frame = data_frame.withColumn("last_update", col("last_update").cast("integer"))
    # grab only newest records
    df_latest = data_frame.filter(data_frame.last_update > last_update)
    if df_latest.count() > 0:
        # grab the new last update value for saving
        last_update_new_row = df_latest.select(max("last_update").alias("last_update"))
        last_update_new = last_update_new_row.select(last_update_new_row.last_update).collect()[0].asDict().get("last_update")
        # save the new last update file to s3
        last_update_temp_file = tempfile.NamedTemporaryFile()
        last_update_file = open(last_update_temp_file.name, 'w')
        last_update_file.write(str(last_update_new))
        # we have to close and reopen this file as binary
        last_update_file.close()
        client.put_object(Bucket=bucketName, Key="trg/" + sub_dir_name + "/last_update", Body=open(last_update_temp_file.name, 'rb'))
        last_update_file.close()
        # cast last update column integer type back to timestamp for saving
        df_latest = df_latest.withColumn("last_update", col("last_update").cast(TimestampType()))
        # save table avro to s3
        path = os.path.join(tempfile.mkdtemp(), "temp_avro")
        df_latest.write.format("avro").save(path)
        index = 0
        for f in os.listdir(path):
            if f.startswith('part'):
                client.put_object(Bucket=bucketName, Key="trg/" + sub_dir_name + "/update_" + str(last_update_new) + "_part" + str(index), Body=open(path + "/" + f, 'rb'))
                index += 1
        return True
    else:
        return False


new_sales_rows = save_new_rows_to_s3("sales_avro", salesAllDf, salesLastUpdate)
new_promotions_rows = save_new_rows_to_s3("promotions_avro", promotionsDf, promotionsLastUpdate)

# new data flag file is used for short circuit operator to decide if any joins or aggregation needs to be done
if new_promotions_rows or new_sales_rows:
    client.put_object(Bucket=bucketName, Key="trg/new_data", Body="")

