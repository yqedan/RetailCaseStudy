from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Run script by using:
# spark-submit2 --packages mysql:mysql-connector-java:5.1.38,com.databricks:spark-avro_2.11:4.0.0 parquetToAggregate3.py

# Main function
def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("RetailCaseStudy") \
        .getOrCreate()

    # read the parquet
    df = spark.read.load("file:///home/cloudera/foodmart/case_study/avroToParquet")
    #print(df.show())

    dfWeekdays = df.filter((col("the_day") == "Monday") | (col("the_day") == "Tuesday") | (col("the_day") == "Wednesday") | (col("the_day") == "Thursday") | (col("the_day") == "Friday"))
    #dfWeekdays.show()
    dfWeekends = df.filter((col("the_day") == "Saturday") | (col("the_day") == "Sunday"))
    #dfWeekends.show()

    # group by tables separately and add a matching column
    groupedDfWeekdays = dfWeekdays.groupBy("region_id","promotion_id", "the_year", "the_month").agg(sum("store_sales").alias("weekday_sales"))
    groupedDfWeekdays = groupedDfWeekdays.withColumn("weekend_sales", groupedDfWeekdays.weekday_sales * 0.0)
    groupedDfWeekends = dfWeekends.groupBy("region_id","promotion_id", "the_year", "the_month").agg(sum("store_sales").alias("weekend_sales"))
    groupedDfWeekends = groupedDfWeekends.withColumn("weekday_sales", groupedDfWeekends.weekend_sales * 0.0)

    #groupedDfWeekdays.show()
    #groupedDfWeekends.show()
    # reorder columns on weekends for union
    groupedDfWeekends = groupedDfWeekends.select("region_id","promotion_id", "the_year", "the_month", "weekday_sales", "weekend_sales")

    # union the table
    unionDf = groupedDfWeekdays.union(groupedDfWeekends)

    # regroup the duplicate keys where store sales happened on both weekdays and weekends
    finalDF = unionDf.groupBy("region_id","promotion_id", "the_year", "the_month").agg(sum("weekday_sales").alias("weekday_sales"),sum("weekend_sales").alias("weekend_sales")).orderBy("region_id", "the_year", "the_month")
    finalDF.show()

    # save to csv file
    finalDF.repartition(1).write.format("csv").mode("overwrite").save("file:///home/cloudera/foodmart/case_study/finalCsv")


# Runs the script
if __name__ == "__main__":
    main()
