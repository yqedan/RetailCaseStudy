# from pyspark import SparkContext
# from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


# Run script by using:
# spark-submit2 --packages mysql:mysql-connector-java:5.1.39,com.databricks:spark-avro_2.11:4.0.0 avro_parquet2.py

# Main function
def main():
    # spark = SparkSession.builder.getOrCreate()
    spark = SparkSession.builder \
        .master("local") \
        .appName("RetailCaseStudy") \
        .getOrCreate()

    # Reading the avro file
    # promotionDF = spark.read.format("com.databricks.spark.avro").load("[DIRECTORY]")
    # not using this now when needed uncomment it
    # sales98decDF = spark.read.format("com.databricks.spark.avro").load("file:///home/cloudera/foodmart/case_study/raw/sales98dec/")

    promoDF = spark.read.format("com.databricks.spark.avro").load(
        "file:///home/cloudera/foodmart/case_study/raw/promotion/")
    sales97DF = spark.read.format("com.databricks.spark.avro").load(
        "file:///home/cloudera/foodmart/case_study/raw/sales97/")
    sales98DF = spark.read.format("com.databricks.spark.avro").load(
        "file:///home/cloudera/foodmart/case_study/raw/sales98/")
    regionDF = spark.read.format("com.databricks.spark.avro").load(
        "file:///home/cloudera/foodmart/case_study/raw/region/")
    timeDF = spark.read.format("com.databricks.spark.avro").load(
        "file:///home/cloudera/foodmart/case_study/raw/timebyday/")
    storeDF = spark.read.format("com.databricks.spark.avro").load(
        "file:///home/cloudera/foodmart/case_study/raw/store/")

    # union Sales 97 + 98
    salesAllDF = sales97DF.unionAll(sales98DF)

    # sales + promotion
    promoSalesDF = salesAllDF.join(promoDF, salesAllDF.promotion_id == promoDF.promotion_id).drop(
        promoDF.promotion_id).drop(promoDF.last_update_date)

    # matching with store id
    promoSalesAllWithStoreIdDF = promoSalesDF.join(storeDF, storeDF.store_id == promoSalesDF.store_id).drop(
        storeDF.store_id).drop(promoSalesDF.last_update_date)

    # matching with time by day
    promoSalesWithStoreTimeDF = promoSalesAllWithStoreIdDF.join(timeDF,
                                                                timeDF.time_id == promoSalesAllWithStoreIdDF.time_id).drop(
        timeDF.time_id)

    print
    salesAllDF.show()
    print
    promoSalesDF.show()
    print
    promoSalesAllWithStoreIdDF.show()
    print
    promoSalesWithStoreTimeDF.show()

    promoSalesWithStoreTimeDF.filter(promoSalesWithStoreTimeDF.promotion_id != "0").repartition(1).write.save(
        "file:///home/cloudera/foodmart/case_study/avroToParquet")


# promoSalesWithStoreTimeDF.filter(promoSalesWithStoreTimeDF.promotion_id != "0")

# Runs the script
if __name__ == "__main__":
    main()
