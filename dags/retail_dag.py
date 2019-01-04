import airflow

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from datetime import timedelta

# To run entire dag:
# shell> airflow resetdb
# shell> airflow webserver -p 9990
# shell> airflow scheduler
# wait for the dag to be triggered by scheduler! and hopefully it wont fail! :)

# To test one task like initial_load for example run this with today's date in YYYY-MM-DD format:
# shell> airflow webserver -p 9990
# shell> airflow test retail_dag initial_load <today's_date>

# To get these airflow libraries for pycharm run this in windows shell
# pip install apache-airflow --no-deps

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG(
    'retail_dag',
    default_args=default_args,
    description='A simple retail DAG',
    schedule_interval=timedelta(days=1)
)

t1 = BashOperator(
    task_id='incremental_load',
    bash_command="spark-submit --packages mysql:mysql-connector-java:5.1.38,org.apache.spark:spark-avro_2.11:2.4.0 /mnt/c/Users/Yusuf/PycharmProjects/RetailCaseStudy/IncrementalLoads.py ",
    dag=dag
)

t2 = BashOperator(
    task_id='avro_parquet',
    bash_command="spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.0  /mnt/c/Users/Yusuf/PycharmProjects/RetailCaseStudy/AVRO_Parquet.py ",
    dag=dag
)

t3 = BashOperator(
    task_id='parquet_agg',
    bash_command="spark-submit /mnt/c/Users/Yusuf/PycharmProjects/RetailCaseStudy/Parquet_Agg.py ",
    dag=dag
)

t4 = BashOperator(
    task_id='initial_load',
    bash_command="spark-submit --packages mysql:mysql-connector-java:5.1.38,org.apache.spark:spark-avro_2.11:2.4.0 /mnt/c/Users/Yusuf/PycharmProjects/RetailCaseStudy/InitialLoads.py ",
    dag=dag
)

t1 >> t2 >> t3
