from pyspark import SparkContext
from pyspark.sql import SQLContext

# Run script by using:
# spark-submit2 --packages mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0 importSourceToRaw1.py

# Main function
def main():
    # Set up spark context
    sc=SparkContext ("local[2]","RetailCapstone")
    # Set up sql context
    sqlContext = SQLContext(sc)
    # Create promotion dataframe. Mysqlconnector package is required for the driver
    # Change url to jdbc:mysql://${HOSTNAME}:3306/${DATABASE_NAME}
    # Change user, dbtable and password accordingly

    url="jdbc:mysql://localhost:3306/food_mart"
    driver="com.mysql.jdbc.Driver"
    user="root"
    password="cloudera"

    promotion_df = sqlContext.read.format ("jdbc").options (url=url,driver=driver,dbtable="promotion",user=user,password=password).load ()
    sales_1997_df = sqlContext.read.format ("jdbc").options (url=url,driver=driver,dbtable="sales_fact_1997",user=user,password=password).load ()
    sales_1998_df = sqlContext.read.format ("jdbc").options (url=url,driver=driver,dbtable="sales_fact_1998",user=user,password=password).load ()
    sales_1998_dec_df = sqlContext.read.format ("jdbc").options (url=url,driver=driver,dbtable="sales_fact_dec_1998",user=user,password=password).load ()

    store_df = sqlContext.read.format ("jdbc").options (url=url,driver=driver,dbtable="store",user=user,password=password).load ()
    region_df = sqlContext.read.format ("jdbc").options (url=url,driver=driver,dbtable="region",user=user,password=password).load ()
    time_by_day_df = sqlContext.read.format ("jdbc").options (url=url,driver=driver,dbtable="time_by_day",user=user,password=password).load ()

    # Just a print statement to see if the dataframe transferred sucessfully
    print promotion_df.show()
    print sales_1997_df.show()
    print sales_1998_df.show()
    print sales_1998_dec_df.show()

    print store_df.show()
    print region_df.show()
    print time_by_day_df.show()

    promotion_df.write.format ("com.databricks.spark.avro").save("file:///home/cloudera/foodmart/case_study/raw/promotion")
    sales_1997_df.write.format ("com.databricks.spark.avro").save("file:///home/cloudera/foodmart/case_study/raw/sales97")
    sales_1998_df.write.format ("com.databricks.spark.avro").save("file:///home/cloudera/foodmart/case_study/raw/sales98")
    sales_1998_dec_df.write.format ("com.databricks.spark.avro").save("file:///home/cloudera/foodmart/case_study/raw/sales98dec")

    store_df.write.format ("com.databricks.spark.avro").save("file:///home/cloudera/foodmart/case_study/raw/store")
    region_df.write.format ("com.databricks.spark.avro").save("file:///home/cloudera/foodmart/case_study/raw/region")
    time_by_day_df.write.format ("com.databricks.spark.avro").save("file:///home/cloudera/foodmart/case_study/raw/timebyday")

# Runs the script
if __name__ == "__main__":
    main()