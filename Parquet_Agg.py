from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Run script by using:
# spark-submit Parquet_Agg.py

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# read the parquet
df = spark.read.load("/home/Yusuf/trg/joined_parquet")

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
finalDF = unionDf.groupBy("region_id", "promotion_id", "the_year", "the_month").agg(first("cost"), sum("weekday_sales").alias("weekday_sales"),sum("weekend_sales").alias("weekend_sales"))

# save your hard work!
finalDF.repartition(1).write.format("csv").mode("overwrite").save("/home/Yusuf/trg/final_csv")
