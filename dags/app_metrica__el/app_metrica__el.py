import os
import json

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

DAG_ID = 'app_metrica__el'

README_FILE_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/repo/dags/{DAG_ID}/README.md"

SLING_FILE_PATH = f'{os.environ["AIRFLOW_HOME"]}/dags/repo/dags/{DAG_ID}/sling_minio_to_clickhouse.yaml'
S3_BUCKET = 'app-metrica'

with open(README_FILE_PATH, "r") as readme_file:
    readme_content = readme_file.read()

with open(SLING_FILE_PATH) as file:
    file.seek(0)
    readme_content = f'''
        {readme_content}\n\n
        # Sling file\n\n
        This DAG uses Sling replication yaml:\n\n
        ```yaml\n{file.read()}\n```'''

s3_conn = BaseHook.get_connection(conn_id='default_minio.raports.net')
clickhouse_conn = BaseHook.get_connection(conn_id='default_clickhouse.raports.net')


with open(README_FILE_PATH, "r") as readme_file:
    readme_content = readme_file.read()


@dag(
    dag_id=DAG_ID,
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['sling', 'minio', 'clickhouse']
)
def app_metrica():
    create_base_table = ClickHouseOperator(
        task_id='create_base_table',
        database='default',
        clickhouse_conn_id=clickhouse_conn.conn_id,
        sql='''
            create table if not exists app_metrica.raw_usage_metrics on cluster default (
                `date` date,
                `event` varchar(30),
                `purchase_sum` decimal(12, 2),
                `os_name` varchar(30),
                `device_id` varchar(30),
                `gender` varchar(30),
                `city` varchar(30),
                `utm_source` varchar(30),
                `_sling_loaded_at` datetime,
                `_sling_stream_url` varchar(255),
                `_sling_row_num` integer,
                `_sling_exec_id` varchar(36)
            )
            engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/app_metrica/raw_usage_metrics', '{replica}')
            partition by `date`
            order by (`os_name`, `date`)
        '''
    )

    create_distributed_table = ClickHouseOperator(
        task_id='create_distributed_table',
        database='default',
        clickhouse_conn_id=clickhouse_conn.conn_id,
        sql='''
            create table if not exists app_metrica.raw_usage_metrics_distributed on cluster default
            as app_metrica.raw_usage_metrics_export
            engine = Distributed(default, app_metrica, raw_usage_metrics, cityHash64(os_name));
        '''
    )

    run_sling = BashOperator(
        task_id='run_sling',
        bash_command=f"sling run -r {SLING_FILE_PATH} -d",
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

    create_base_table >> create_distributed_table >> run_sling

app_metrica()