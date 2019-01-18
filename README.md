# Retail Case Study

### Objectives:

##### In this project, we will be migrating an [existing](https://github.com/yqedan/RetailCaseStudyHadoop) Retail Case Study project to use a New Architecture using PySpark, Apache Airflow and Snowflake.

### Setup:

 * Enable Windows Subsystem for Linux in Windows Features
 * Install Ubuntu 18.04 LTS in Microsoft Store
 * Clone repo into your home directory and execute setup.sh NOTE (You will need root access so enter your password after executing and let installation run)
 * Execute setup_tables.sh to alter food_mart table
 * You will need to configure your S3 bucket and setup boto3 credentials [instructions here](https://github.com/boto/boto3)
 * Open setup_bucket.py and change the bucketName variable.
 * Create a snowflake credentials file with your account (or skip this part and comment that task out in the DAG):
   ```
   $ echo "<snowflake user>,<snowflake password>,<snowflake_account>" >> ~/.snowflake_credentials
   ```
 * Copy the retail_dag in this repo into your airflow dags folder: (You can configure airflow to point to this repo in ~/airflow/airflow.cfg if you want to instead)
   ```
   $ cp -r ~/RetailCaseStudy/dags/ ~/airflow/dags/
   ```
 * If needed start the postgres and mysql services: (did this in setup.sh but will be needed if the services go down)
   ```
   $ sudo service mysql start
   $ sudo service postgresql start
   ```
 * Start the airflow web server and scheduler in separate terminals:
   ```
   $ airflow webserver -p 9990
   $ airflow scheduler
   ```
 * Switch on the dag in the web UI and it watch it run:
 ![Imgur](https://i.imgur.com/IzEJDCM.png)
 
### Assignment:

  * Find total Promotion sales generated on weekdays and weekends for each region, year & month
  * Find the most popular promotion which generated highest sales in each region

### Steps Involved:

  *	Create PySpark scripts for initial and incremental loads. The script will read sales and promotion tables based on last_update_date column from mysql and store them in AVRO format in S3 buckets. You might want to add a last_update_date in the tables
  *	A second PySpark script will read the AVRO files, filter out all non-promotion records from input, join the promotion and sales tables and save the data in Parquet format in S3 buckets.
  *	The Parquet file is aggregated by regionID, promotionID, sales_year, sales_month to generate total StoreSales for weekdays and weekends and the output is saved as a CSV file in S3 buckets.
  *	The CSV file generated is loaded into a Snowflake database.
  *	Following queries are executed on the Snowflake table

    * Query1: List the total weekday sales & weekend sales for each promotions:
                   Following columns are required in output:
                   Region ID, Promotion ID, Promotion Cost, total weekday sales, total weekend sales
    * Query 2: List promotions, which generated highest total sales (weekday + weekend) in each region. Following columns are required in output: Region ID, Promotion ID, Promotion Cost, total sales
  *	Automate the workflow using Airflow scheduler
