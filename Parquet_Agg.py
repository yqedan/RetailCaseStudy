import boto3
import os
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Run script by using:
# spark-submit Parquet_Agg.py

spark = SparkSession.builder \
 .master("local") \
 .appName("Parquet_Aggregation") \
 .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

client = boto3.client('s3')
resource = boto3.resource('s3')
bucketName = "bhuvabucket"
bucket = resource.Bucket(bucketName)


def read_parquet_from_s3():
    for obj in bucket.objects.all():
        if obj.key == "trg/joined_parquet":
            file = tempfile.NamedTemporaryFile(delete=False)
            file.write(obj.get()['Body'].read())
            file.close()
            return spark.read.load(file.name)


df = read_parquet_from_s3()

# split weekday and weekend records into 2 tables for aggregation
dfWeekdays = df.filter((col("the_day") == "Monday") | (col("the_day") == "Tuesday") | (col("the_day") == "Wednesday") | (col("the_day") == "Thursday") | (col("the_day") == "Friday"))
dfWeekends = df.filter((col("the_day") == "Saturday") | (col("the_day") == "Sunday"))

# group by tables separately and add a matching column
groupedDfWeekdays = dfWeekdays.groupBy("region_id", "promotion_id", "the_year", "the_month").agg(first("cost").alias("cost"), sum("store_sales").alias("weekday_sales"))
groupedDfWeekdays = groupedDfWeekdays.withColumn("weekend_sales", groupedDfWeekdays.weekday_sales * 0.0)
groupedDfWeekends = dfWeekends.groupBy("region_id", "promotion_id", "the_year", "the_month").agg(first("cost").alias("cost"), sum("store_sales").alias("weekend_sales"))
groupedDfWeekends = groupedDfWeekends.withColumn("weekday_sales", groupedDfWeekends.weekend_sales * 0.0)

# reorder columns on weekends for union
groupedDfWeekends = groupedDfWeekends.select("region_id", "promotion_id", "the_year", "the_month", "cost", "weekday_sales", "weekend_sales")

# union the table
unionDf = groupedDfWeekdays.union(groupedDfWeekends)

# regroup the duplicate keys where store sales happened on both weekdays and weekends
finalDF = unionDf.groupBy("region_id", "promotion_id", "the_year", "the_month").agg(first("cost"), sum("weekday_sales").alias("weekday_sales"), sum("weekend_sales").alias("weekend_sales"))

# save new csv to temp directory
path = os.path.join(tempfile.mkdtemp(), "final_csv")
finalDF.repartition(1).write.format("csv").save(path)

# select and save just the csv file to s3 which is always the 3rd position in the directory
parquet_file = os.listdir(path)[3]
client.put_object(Bucket=bucketName, Key="trg/final_csv", Body=open(path + "/" + parquet_file, 'rb'))
