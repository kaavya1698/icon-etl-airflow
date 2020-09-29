from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator



default_args = {
    'owner' : 'airflow',
    'depend_on_past' : False,
    'start_date' : datetime(2020, 9, 28, 00, 00, 00),
    'retries': 5,
    'retry_delay' : timedelta(minutes=5)
}

def get_postgres_data():
    request = "SELECT * FROM blocks LIMIT 10" #double check how to write this
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="postgres") #made this connection in Airflow UI
    connection = pg_hook.get_conn() #gets the connection from postgres
    cursor = connection.cursor() #cursor to postgres database
    cursor.execute(request) #executes request
    sources = cursor.fetchall() #fetches all the data from the executed request


with DAG('load_rds', default_args=default_args, schedule_interval = "@once", catchup=False) as dag:

    start_task = DummyOperator(task_id = 'start_task')
    hook_task = PythonOperator(task_id='load_rds', python_callable = get_postgres_data)
    start_task >> hook_task