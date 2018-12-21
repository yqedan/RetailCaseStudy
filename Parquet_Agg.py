from pyspark.sql import SparkSession
#from pyspark.sql.Column import *
#from pyspark.sql.functions import *

#Run script by using:
#spark-submit --packages mysql:mysql-connector-java:5.1.38,com.databricks:spark-avro_2.11:4.0.0 AVRO_Parquet.py
# columns 4 = the_day
# columns 5 =
spark = SparkSession.builder.getOrCreate()

df = spark.read.load("/home/Yusuf/joined_parquet")
#df.show()
groupedDf = df.groupBy("region_id","promotion_id","the_year","the_month").agg({'store_sales': 'sum'})


print("\n\n\n\n\n" + groupedDf.show() + "\n\n\n\n\n")
