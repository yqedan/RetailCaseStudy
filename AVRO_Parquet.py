from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Run script by using:
# spark-submit --packages com.databricks:spark-avro_2.11:4.0.0 AVRO_Parquet.py

spark = SparkSession.builder.getOrCreate()

promotionDF = spark.read.format("com.databricks.spark.avro").load("/home/Yusuf/trg/promotions_avro")
salesDF = spark.read.format("com.databricks.spark.avro").load("/home/Yusuf/trg/sales_avro")
timeDF = spark.read.format("com.databricks.spark.avro").load("/home/Yusuf/trg/timeByDay_avro")
storeDF = spark.read.format("com.databricks.spark.avro").load("/home/Yusuf/trg/store_avro")

SaSt_DF = salesDF.join(storeDF, salesDF.store_id == storeDF.store_id)

STS_DF = SaSt_DF.join(timeDF, SaSt_DF.time_id == timeDF.time_id)

joinedDF = promotionDF.join(STS_DF, promotionDF.promotion_id == STS_DF.promotion_id)

finalDF = joinedDF.select(promotionDF.promotion_id.cast(IntegerType()),
                          storeDF.region_id.cast(IntegerType()),
                          timeDF.the_year.cast(IntegerType()),
                          timeDF.the_month,
                          timeDF.the_day,
                          salesDF.last_update,
                          promotionDF.cost.cast(FloatType()),
                          salesDF.store_sales.cast(FloatType()))

finalDF.filter(promotionDF.promotion_id != "0").repartition(1).write.mode("overwrite").save("/home/Yusuf/trg/joined_parquet")
