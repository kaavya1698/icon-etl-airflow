from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
 
default_args = {
    'owner' : 'airflow',
    'depend_on_past' : False,
    'start_date' : datetime(2020, 9, 28, 00, 00, 00),
    'retries': 5,
    'retry_delay' : timedelta(minutes=5)
}
 

dag = DAG('upload_redshift', default_args=default_args, schedule_interval = '0 8 * * *', catchup=False)

transfer_redshift= S3ToRedshiftTransfer(
    task_id='transfer_redshift',
    schema='schema',
    table= 'block_test',
    s3_bucket='icon-redshift-dump-dev',
    redshift_conn_id = 'icon-analytics-dev',
    default_args= 'default_args'
    dag = dag
    )

transfer_redshift