from datetime import datetime, timedelta
from tempfile import TemporaryFile
import pandas as pd 

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import BashOperator
from airflow.models import Variable

start_date = Variable.get("rds_s3_start_date")

default_args = {
    'owner' : 'airflow',
    'depend_on_past' : False,
    'start_date' : start_date,
    'retries': 5,
    'retry_delay' : timedelta(minutes=5)
}


def get_postgres_block_data():
    request = "SELECT * FROM logs" #double check how to write this
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="postgres") #made this connection in Airflow UI
    connection = pg_hook.get_conn() #gets the connection from postgres
    cursor = connection.cursor() #cursor to postgres database
    cursor.execute(request) #executes request
    sources = cursor.fetchall() #fetches all the data from the executed request
    results = pd.DataFrame(sources) #writes to datafram
    print(results)
    results.to_csv('/home/ubuntu/s3_dump/block_dump.csv') #printing to dir owned by airflow. Need to change this to temp dir but can be done later

def upload_data_to_S3(filename, key, bucket_name):
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

def get_date(ds, **kwargs):
    Variable.set('execution_date', kwargs['execution_date'])
    return

with DAG('load_rds_s3', default_args=default_args, schedule_interval = '@once', catchup=False) as dag:

    start_task = DummyOperator(task_id = 'start_task')
    load_block_rds_task = PythonOperator(task_id='load_block_rds', python_callable = get_postgres_block_data)
    upload_blocks_to_s3_task = PythonOperator(task_id='upload_blocks_to_S3', python_callable = upload_data_to_S3, op_kwargs={'filename': '/home/ubuntu/s3_dump/block_dump.csv', 'key':'block_rds_dump', 'bucket_name': 'icon-redshift-dump-dev'})

    start_task >> load_block_rds_task >> upload_blocks_to_s3_task






