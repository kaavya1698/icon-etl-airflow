from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator)
from helpers import SqlQueries
from airflow.models import Variable
from create_table_redshift import *

start_date = Variable.get("s3_redshift_start_date")


default_args = {
    'owner': 'airflow',
    'start_date': start_date, 
    'schedule_interval': None,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry':False
}


dag = DAG('s3_to_redshift',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup=False,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# start-date:   'log_data/2018/11/2018-11-01-events.json'
# end-date:     'log_data/2018/11/2018-11-30-events.json'


blocks_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="icon.blocks",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials_root",
    s3_bucket="icon-redshift-dump-dev",
    s3_key="log_data",
    #s3_key='log_data/2018/11/{}-{}-{}-events.json',
    region = "us-east-1",
    create_table = blocks_table_create,
    format = "csv",
    provide_context=True
)

start_operator >> blocks_to_redshift