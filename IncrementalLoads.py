import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.38,com.databricks:spark-avro_2.11:4.0.0 IncrementalLoads.py

# read in the file to determine the last update timestamp
try:
    lastUpdateFile = open("/home/Yusuf/trg/last_update")
    lastUpdate = int(lastUpdateFile.readline())
    lastUpdateFile.close()
except IOError:
    print("Error: can\'t find file or read data maybe run an initial load first?")
    sys.exit()

spark = SparkSession.builder \
 .master("local") \
 .appName("Incremental_Loads_For_Retail_Agg") \
 .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

url = "jdbc:mysql://localhost:3306/food_mart"
driver = "com.mysql.jdbc.Driver"
user = "root"
password = "root"

salesAllTable = "food_mart.sales_fact_all"

salesAllDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=salesAllTable, user=user, password=password).load()
# select all but cast date column timestamp to integer for filter logic
salesAllDf = salesAllDf.select("product_id", "time_id", "customer_id", "promotion_id", "store_id", "store_sales", "store_cost", "unit_sales", col("last_update").cast("integer"))
# grab only newest records
salesAllDfLatest = salesAllDf.filter(salesAllDf.last_update > lastUpdate)
# append to directory
salesAllDfLatest.write.format("com.databricks.spark.avro").mode("append").save("/home/Yusuf/trg/sales_avro")
