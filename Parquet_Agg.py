from pyspark.sql import SparkSession
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext

# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.38,com.databricks:spark-avro_2.11:4.0.0 AVRO_Parquet.py

# def main():
#
#     if df.the_day == "Saturday":
#         df.withColumn("weekend_sales")
#
#     elif df.the_day == "Sunday":
#
#         else df.withColumn("weekday_sales")
#
# if __name__ == '__main__':
#     spark = SparkSession.builder.getOrCreate()
#
#     df = spark.read.load("[DIRECTORY]").
#
#     main()