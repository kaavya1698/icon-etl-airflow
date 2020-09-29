from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
import airflow.hooks.S3_hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from tempfile import TemporaryFile

import pandas as pd 



default_args = {
    'owner' : 'airflow',
    'depend_on_past' : False,
    'start_date' : datetime(2020, 9, 28, 00, 00, 00),
    'retries': 5,
    'retry_delay' : timedelta(minutes=5)
}

def get_postgres_data(tempfile):
    request = "SELECT * FROM blocks LIMIT 10" #double check how to write this
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="postgres") #made this connection in Airflow UI
    connection = pg_hook.get_conn() #gets the connection from postgres
    #result = connection.get_pandas_df(request)
    #result.to_csv(tempfile)

    cursor = connection.cursor() #cursor to postgres database
    cursor.execute(request) #executes request
    sources = cursor.fetchall() #fetches all the data from the executed request
    df = pd.DataFrame(sources)
    df.to_csv('tempfile')

def upload_data_to_S3(file_name, bucket_name):
    hook = airflow.hooks.S3_hook.S3_hook('s3_conn')
    hook.load_file(file_name, bucket_name)

def run_export_to_s3():
    tempfile = TemporaryFile()
    get_postgres_data(tempfile)
    upload_data_to_S3(tempfile, "icon-redshift-dump-dev")


with DAG('load_rds_s3', default_args=default_args, schedule_interval = "@once", catchup=False) as dag:

    start_task = DummyOperator(task_id = 'start_task')
    hook_task = PythonOperator(task_id='load_rds_s3', python_callable = run_export_to_s3)
    start_task >> hook_task