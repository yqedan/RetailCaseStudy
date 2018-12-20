from pyspark.sql import SparkSession

#To test this: shell>spark-submit --jars <jdbc driver jar> InitalLoads.py

spark = SparkSession.builder \
 .master("local") \
 .appName("Retail Case Study") \
 .getOrCreate()

url="jdbc:mysql://localhost:3306/food_mart"
driver = "com.mysql.jdbc.Driver"
password = "root"

salesAllTable = "food_mart.sales_fact_all"
promotionsTable = "food_mart.promotion"
timeByDayTable = "food_mart.time_by_day"
storeTable = "food_mart.store"


salesAllDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=salesAllTable, user="root", password=password).load()
promotionsDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=promotionsTable, user="root", password=password).load()
timeDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=timeByDayTable, user="root", password=password).load()
storeDf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=storeTable, user="root", password=password).load()

salesAllDf.write.format("com.databricks.spark.avro").save("/home/Yusuf/sales_avro")
promotionsDf.write.format("com.databricks.spark.avro").save("/home/Yusuf/promotions_avro")
timeDf.write.format("com.databricks.spark.avro").save("/home/Yusuf/timeByDay_avro")
storeDf.write.format("com.databricks.spark.avro").save("/home/Yusuf/store_avro")

