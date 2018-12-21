from pyspark.sql import SparkSession

spark = SparkSession.builder \
 .master("local") \
 .appName("Retail Case Study") \
 .getOrCreate()

url="jdbc:mysql://localhost:3306/food_mart"
driver = "com.mysql.jdbc.Driver"
password = "root"

