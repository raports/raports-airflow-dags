import os
import json

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime


DAG_ID = 'app_metrica__el'

SLING_FILE_PATH = f'{os.environ["AIRFLOW_HOME"]}/dags/repo/dags/{DAG_ID}/sling_minio_to_clickhouse.yaml'
S3_BUCKET = 'app-metrica'

s3_conn = BaseHook.get_connection(conn_id='default_minio.raports.net')
clickhouse_conn = BaseHook.get_connection(conn_id='default_clickhouse.raports.net')

@dag(
    dag_id=DAG_ID,
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['sling']
)
def app_metrica():
    sling_task = BashOperator(
        task_id='run_sling',
        bash_command=f"sling run -r {SLING_FILE_PATH}",
        env={
            'PATH': '/home/airflow/.local/bin',
            'minio_app_metrica': f'''{{
                type: s3, 
                bucket: 'app-metrica', 
                access_key_id: {s3_conn.login}, 
                secret_access_key: "{s3_conn.password}", 
                endpoint: {json.loads(s3_conn.extra).get("endpoint_url", "") if s3_conn.extra else ""}
            }}''',
            'clickhouse': f'clickhouse://{clickhouse_conn.login}:{clickhouse_conn.password}@{clickhouse_conn.host}:{clickhouse_conn.port}/{clickhouse_conn.schema}'
        }
    )

app_metrica()