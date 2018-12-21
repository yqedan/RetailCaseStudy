from pyspark.sql import SparkSession

# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.38,com.databricks:spark-avro_2.11:4.0.0 AVRO_Parquet.py

spark = SparkSession.builder.getOrCreate()

promotionDF = spark.read.format("com.databricks.spark.avro").load("[DIRECTORY]")
salesDF = spark.read.format("com.databricks.spark.avro").load("[DIRECTORY]")
timeDF = spark.read.format("com.databricks.spark.avro").load("[DIRECTORY]")
storeDF = spark.read.format("com.databricks.spark.avro").load("[DIRECTORY]")

SaSt_DF = salesDF.join(storeDF, salesDF.store_id == storeDF.store_id)

STS_DF = SaSt_DF.join(timeDF, SaSt_DF.time_id == timeDF.time_id)

joinedDF = promotionDF.join(STS_DF, promotionDF.promotion_id == STS_DF.promotion_id)

finalDF = joinedDF.select(promotionDF.promotion_id,
                          storeDF.region_id,
                          timeDF.the_year,
                          timeDF.the_month,
                          timeDF.the_day,
                          salesDF.last_update,
                          promotionDF.cost,
                          salesDF.store_sales)

finalDF.filter(promotionDF.promotion_id != "0").repartition(1).write.save("[DIRECTORY]")


