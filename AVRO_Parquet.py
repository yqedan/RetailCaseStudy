from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext

# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.38,com.databricks:spark-avro_2.11:4.0.0 AVRO_Parquet.py

spark = SparkSession.builder.getOrCreate()

#promotionDF = spark.read.format("com.databricks.spark.avro").load("~")
salesDF = spark.read.format("com.databricks.spark.avro").load("/home/Yusuf/test_avro")
salesDF.show()

#joinedDF = promotionDF.join(salesDF, promotionDF.promotion_id == salesDF.promotion_id).drop(salesDF.promotion_id)

joinedDF.filter(promotionDF.promotion_id != "0").repartition(1).write.save("[DIRECTORY]")


