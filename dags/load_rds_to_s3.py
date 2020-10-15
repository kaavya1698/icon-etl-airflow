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
    request = "SELECT * FROM blocks" #double check how to write this
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="postgres") #made this connection in Airflow UI
    connection = pg_hook.get_conn() #gets the connection from postgres
    cursor = connection.cursor() #cursor to postgres database
    cursor.execute(request) #executes request
    sources = cursor.fetchall() #fetches all the data from the executed request
    results = pd.DataFrame(sources) #writes to datafram
    print(results)
    results.to_csv('/home/ubuntu/s3_dump/blocks_dump.csv', index=False) #printing to dir owned by airflow. Need to change this to temp dir but can be done later

def get_postgres_transactions_data():
    trans_request = "SELECT * FROM transactions" #double check how to write this
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="postgres") #made this connection in Airflow UI
    connection = pg_hook.get_conn() #gets the connection from postgres
    trans_cursor = connection.cursor() #cursor to postgres database
    trans_cursor.execute(trans_request) #executes request
    trans_sources = trans_cursor.fetchall() #fetches all the data from the executed request
    trans_results = pd.DataFrame(trans_sources) #writes to datafram
    trans_results = trans_results["nid"].astype(int)
    print(trans_results)
    trans_results.to_csv('/home/ubuntu/s3_dump/transactions_dump.csv', index=False) #printing to dir owned by airflow. Need to change this to temp dir but can be done later

def get_postgres_receipts_data():
    receipts_request = "SELECT * FROM receipts" #double check how to write this
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="postgres") #made this connection in Airflow UI
    connection = pg_hook.get_conn() #gets the connection from postgres
    receipts_cursor = connection.cursor() #cursor to postgres database
    receipts_cursor.execute(receipts_request) #executes request
    receipts_sources = receipts_cursor.fetchall() #fetches all the data from the executed request
    receipts_results = pd.DataFrame(receipts_sources) #writes to datafram
    print(receipts_results)
    receipts_results.to_csv('/home/ubuntu/s3_dump/receipts_dump.csv', index=False) #printing to dir owned by airflow. Need to change this to temp dir but can be done later

def get_postgres_logs_data():
    logs_request = "SELECT * FROM logs" #double check how to write this
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="postgres") #made this connection in Airflow UI
    connection = pg_hook.get_conn() #gets the connection from postgres
    logs_cursor = connection.cursor() #cursor to postgres database
    logs_cursor.execute(logs_request) #executes request
    logs_sources = logs_cursor.fetchall() #fetches all the data from the executed request
    logs_results = pd.DataFrame(logs_sources) #writes to datafram
    print(logs_results)
    logs_results.to_csv('/home/ubuntu/s3_dump/logs_dump.csv', index=False) #printing to dir owned by airflow. Need to change this to temp dir but can be done later

def upload_data_to_S3(filename, key, bucket_name):
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG('load_rds_s3', default_args=default_args, schedule_interval = '@once', catchup=False) as dag:

    start_task = DummyOperator(task_id = 'start_task')
    pause_task = DummyOperator(task_id = 'pause_task')
    end_task = DummyOperator(task_id = 'end_task')

    #blocks
    load_block_rds_task = PythonOperator(task_id='load_block_rds', python_callable = get_postgres_block_data)
    upload_blocks_to_s3_task = PythonOperator(task_id='upload_blocks_to_S3', python_callable = upload_data_to_S3, op_kwargs={'filename': '/home/ubuntu/s3_dump/blocks_dump.csv', 'key':'blocks.csv', 'bucket_name': 'icon-redshift-dump-dev'})

    #transactions
    load_transactions_rds_task = PythonOperator(task_id='load_transactions_rds', python_callable = get_postgres_transactions_data)
    upload_transactions_to_s3_task = PythonOperator(task_id='upload_transactions_to_S3', python_callable = upload_data_to_S3, op_kwargs={'filename': '/home/ubuntu/s3_dump/transactions_dump.csv', 'key':'transactions.csv', 'bucket_name': 'icon-redshift-dump-dev'})

    #receipts
    load_receipts_rds_task = PythonOperator(task_id='load_receipts_rds', python_callable = get_postgres_receipts_data)
    upload_receipts_to_s3_task = PythonOperator(task_id='upload_receipts_to_S3', python_callable = upload_data_to_S3, op_kwargs={'filename': '/home/ubuntu/s3_dump/receipts_dump.csv', 'key':'receipts.csv', 'bucket_name': 'icon-redshift-dump-dev'})

    #logs
    #load_logs_rds_task = PythonOperator(task_id='load_logs_rds', python_callable = get_postgres_logs_data)
    #upload_logs_to_s3_task = PythonOperator(task_id='upload_logs_to_S3', python_callable = upload_data_to_S3, op_kwargs={'filename': '/home/ubuntu/s3_dump/logs_dump.csv','key_prefix':'logs', 'key':'_rds_dump', 'bucket_name': 'icon-redshift-dump-dev'})


    start_task >> load_block_rds_task >> upload_blocks_to_s3_task >> pause_task >> load_receipts_rds_task >> upload_receipts_to_s3_task >> end_task
    start_task >> load_transactions_rds_task >> upload_transactions_to_s3_task >> pause_task





