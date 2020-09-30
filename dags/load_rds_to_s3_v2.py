from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
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
    request = "SELECT * FROM blocks LIMIT 25" #double check how to write this
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="postgres") #made this connection in Airflow UI
    connection = pg_hook.get_conn() #gets the connection from postgres
    #result = connection.get_pandas_df(request)
    #result.to_csv(tempfile)

    cursor = connection.cursor() #cursor to postgres database
    cursor.execute(request) #executes request
    sources = cursor.fetchall() #fetches all the data from the executed request
    results = pd.DataFrame(sources)
    print(results)
    results.to_csv(tempfile)


def upload_data_to_S3(filename, key, bucket_name):
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


def run_export_to_s3():
    result_file = TemporaryFile()
    get_postgres_data('result_file')
    upload_data_to_S3('result_file', 'my_s3_file_v2.csv', 'icon-redshift-dump-dev')
    #delete tempfile.csv


with DAG('load_rds_s3_v2', default_args=default_args, schedule_interval = "@once", catchup=False) as dag:


    start_task = DummyOperator(task_id = 'start_task')
    #load_rds_task = PythonOperator(task_id='load_rds', python_callable = get_postgres_data)
    #upload_to_s3_task = PythonOperator(task_id='upload_to_S3', python_callable = upload_data_to_S3, op_kwargs={'filename': '/home/ubuntu/s3_dump/test.csv', 'key':'my_s3_file.csv', 'bucket_name': 'icon-redshift-dump-dev'})
    run_export_to_s3 = PythonOperator(task_id = 'run_export_to_s3', python_callable = run_export_to_s3)
    start_task >> run_export_to_s3





