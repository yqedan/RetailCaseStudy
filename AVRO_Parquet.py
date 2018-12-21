from pyspark.sql import SparkSession

# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.38,com.databricks:spark-avro_2.11:4.0.0 AVRO_Parquet.py

spark = SparkSession.builder.getOrCreate()

promotionDF = spark.read.format("com.databricks.spark.avro").load("/home/Yusuf/promotions_avro")
salesDF = spark.read.format("com.databricks.spark.avro").load("/home/Yusuf/sales_avro")
timeDF = spark.read.format("com.databricks.spark.avro").load("/home/Yusuf/timeByDay_avro")
storeDF = spark.read.format("com.databricks.spark.avro").load("/home/Yusuf/store_avro")

SaSt_DF = salesDF.join(storeDF, salesDF.store_id == storeDF.store_id).drop(storeDF.store_id)

STS_DF = SaSt_DF.join(timeDF, SaSt_DF.time_id == timeDF.time_id).drop(timeDF.time_id)

joinedDF = promotionDF.join(STS_DF, promotionDF.promotion_id == STS_DF.promotion_id).drop(STS_DF.promotion_id)

joinedDF.filter(promotionDF.promotion_id != "0").repartition(1).write.save("/home/Yusuf/joined_parquet")


