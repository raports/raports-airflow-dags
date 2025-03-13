import os
import json

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime


DAG_ID = 'app_metrica'

SLING_FILE_PATH = f'{os.environ["AIRFLOW_HOME"]}/dags/repo/dags/{DAG_ID}/sling_minio_to_dwh.yaml'
S3_BUCKET = 'app-metrica'

s3_conn = BaseHook.get_connection(conn_id='default_minio.raports.net')
dwh_conn = BaseHook.get_connection(conn_id='dwh_postgresql.raports.net')

@dag(
    dag_id=DAG_ID,
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 1),
    catchup=False
)
def app_metrica():
    sling_task = BashOperator(
        task_id='run_sling',
        bash_command=f"sling run -r {SLING_FILE_PATH}",
        env={
            'PATH': '/home/airflow/.local/bin',
            'MINIO': f'''{{
                type: s3, 
                bucket: {S3_BUCKET}, 
                access_key_id: {s3_conn.login}, 
                secret_access_key: "{s3_conn.password}", 
                endpoint: {json.loads(s3_conn.extra).get("endpoint_url", "") if s3_conn.extra else ""}
            }}''',
            'DWH': f'postgresql://{dwh_conn.login}:{dwh_conn.password}@{dwh_conn.host}:5432/{dwh_conn.schema}?sslmode=disable'
        }
    )

app_metrica()