import airflow
import boto3

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
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

resource = boto3.resource('s3')
bucketName = "yusufqedanbucket"
bucket = resource.Bucket(bucketName)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG(
    'retail_dag',
    default_args=default_args,
    description='A simple retail DAG',
    schedule_interval=timedelta(minutes=1),
    max_active_runs=1,
    catchup=False
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

inc = BashOperator(
    task_id='incremental_load',
    bash_command="spark-submit --packages mysql:mysql-connector-java:5.1.38,org.apache.spark:spark-avro_2.11:2.4.0 ~/RetailCaseStudy/IncrementalLoads.py ",
    dag=dag
)

av_par = BashOperator(
    task_id='avro_parquet',
    bash_command="spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.0  ~/RetailCaseStudy/AVRO_Parquet.py ",
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

par_agg = BashOperator(
    task_id='parquet_agg',
    bash_command="spark-submit ~/RetailCaseStudy/Parquet_Agg.py ",
    dag=dag
)

init = BashOperator(
    task_id='initial_load',
    bash_command="spark-submit --packages mysql:mysql-connector-java:5.1.38,org.apache.spark:spark-avro_2.11:2.4.0 ~/RetailCaseStudy/InitialLoads.py ",
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag
)


def any_new_rows():
    for obj in bucket.objects.all():
        key = obj.key
        if key == "trg/new_data":
            return True
    return False


any_new_rows_task = ShortCircuitOperator(
    task_id='any_new_rows',
    python_callable=any_new_rows,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

csv_snowflake = BashOperator(
    task_id="csv_snowflake",
    bash_command="python3 ~/RetailCaseStudy/save_csv_to_snowflake.py ",
    dag=dag
)

finish = DummyOperator(
    task_id="finish",
    dag=dag
)

start >> inc
inc >> init
init >> av_par
inc >> any_new_rows_task
any_new_rows_task >> av_par
av_par >> par_agg
par_agg >> csv_snowflake
csv_snowflake >> finish
