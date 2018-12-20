from pyspark.sql import SparkSession

#To test this: shell>spark-submit --jars <jdbc driver jar> InitalLoads.py

spark = SparkSession.builder \
 .master("local") \
 .appName("Retail Case Study") \
 .getOrCreate()

url="jdbc:mysql://localhost:3306/food_mart"
driver = "com.mysql.jdbc.Driver"
salesAlltable = "food_mart.sales_fact_all"

salesalldf = spark.read.format("jdbc").options(url=url, driver=driver, dbtable=salesAlltable, user="root", password="root").load()

salesalldf.createTempView("sales_table_all")

dfTest = spark.sql("select * from sales_table_all where promotion_id == 1 and store_id = 1 and store_cost == 1.0")
dfTest.show()

print("The count is : " + str(salesalldf.count()))

