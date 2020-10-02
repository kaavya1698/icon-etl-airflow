from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

start_date = Variable.get("s3_redshift_start_date")

default_args = {
    'owner' : 'airflow',
    'depend_on_past' : False,
    'start_date' : start_date,
    'retries': 5,
    'retry_delay' : timedelta(minutes=5)
}

dag = DAG('upload_redshift', default_args=default_args, schedule_interval = '0 8 * * *', catchup=False)

start_task = DummyOperator(task_id = 'start_task', dag = dag)

transfer_redshift = S3ToRedshiftTransfer(
	task_id='transfer_redshift',
    schema='schema',
    table= 'block_test',
    s3_bucket='icon-redshift-dump-dev',
    redshift_conn_id = 'icon-analytics-dev',
    default_args= 'default_args',
    dag = dag
    )

start_task >> transfer_redshift