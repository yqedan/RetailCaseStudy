from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Run script by using:
#spark-submit --packages mysql:mysql-connector-java:5.1.38,com.databricks:spark-avro_2.11:4.0.0 AVRO_Parquet.py

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# read the parquet
df = spark.read.load("/home/Yusuf/joined_parquet")

# for debugging only! the final table should have equal number of rows (408) if we just simply add the total sales!
#groupedDfTot = df.groupBy("region_id", "promotion_id", "the_year", "the_month").agg(sum("store_sales").alias("total_sales"))

# split weekday and weekend records into 2 tables for aggregation
dfWeekdays = df.filter((col("the_day") == "Monday") | (col("the_day") == "Tuesday") | (col("the_day") == "Wednesday") | (col("the_day") == "Thursday") | (col("the_day") == "Friday"))
dfWeekends = df.filter((col("the_day") == "Saturday") | (col("the_day") == "Sunday"))

# group by tables separately and add a matching column
groupedDfWeekdays = dfWeekdays.groupBy("region_id", "promotion_id", "the_year", "the_month").agg(sum("store_sales").alias("weekday_sales"))
groupedDfWeekdays = groupedDfWeekdays.withColumn("weekend_sales", groupedDfWeekdays.weekday_sales * 0.0)
groupedDfWeekends = dfWeekends.groupBy("region_id", "promotion_id", "the_year", "the_month").agg(sum("store_sales").alias("weekend_sales"))
groupedDfWeekends = groupedDfWeekends.withColumn("weekday_sales", groupedDfWeekends.weekend_sales * 0.0)

# reorder columns on weekends for union
groupedDfWeekends = groupedDfWeekends.select("region_id", "promotion_id", "the_year", "the_month", "weekday_sales", "weekend_sales")

# union the table
unionDf = groupedDfWeekdays.union(groupedDfWeekends)

# regroup the duplicate keys where store sales happened on both weekdays and weekends
finalDF = unionDf.groupBy("region_id", "promotion_id", "the_year", "the_month").agg(sum("weekday_sales").alias("weekday_sales"),sum("weekend_sales").alias("weekend_sales"))

# save your hard work!
finalDF.repartition(1).write.format("csv").save("/home/Yusuf/final_csv")

# all my debugging and proof that it works!
# There were 39 rows where sales happened on both weekdays and weekends with the given key which is why we did the 3rd group by
# print("weekdays grouped")
# print(groupedDfWeekdays.count())
# print("weekends grouped")
# print(groupedDfWeekends.count())
# print("total grouped")
# print(groupedDfTot.count())
# print("weekdays")
# print(groupedDfWeekdays.show())
# print("weekends")
# print(groupedDfWeekends.show())
# print("union")
# print(unionDf.show())
# print("final grouped count")
# print(finalDF.count())
# print("final grouped table")
# print(finalDF.show())
# print("final grouped table rows with sales on both weekdays and weekends")
# finalTest = finalDF.where((col("weekday_sales") > 0.0) & (col("weekend_sales") > 0.0))
# print(finalTest.show())
# print(finalTest.count())
