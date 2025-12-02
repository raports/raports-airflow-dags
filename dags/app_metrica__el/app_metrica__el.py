import os
import glob
import json
import yaml
from pendulum import datetime, duration

from airflow import Dataset
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


# Specify main DAG parameters
DAG_ID = "app_metrica__el"
DAG_FOLDER_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/repo/dags/{DAG_ID}"

# Define target connection (one per DAG)
target_conn = BaseHook.get_connection(conn_id="clickhouse_raports_io")

# Load README content
readme_content = ""
readme_file_path = f"{DAG_FOLDER_PATH}/README.md"
if os.path.exists(readme_file_path):
    with open(readme_file_path, "r") as readme_file:
        readme_content = readme_file.read()

# Find all sling*.yaml files in the DAG folder and append their content to the README
sling_files = glob.glob(os.path.join(DAG_FOLDER_PATH, "sling*.yaml"))
if sling_files:
    readme_content += f"\n\n---\n## Sling файлы используемый в DAG'е\n"
    for sling_file in sling_files:
        with open(sling_file, "r") as f:
            content = f.read()
            readme_content += f"\n\n---\n### `{os.path.basename(sling_file)}`\n```yaml\n{content}\n```\n"

# Prepare output Datasets based on sling files
result_datasets = {}
if sling_files:
    for sling_file in sling_files:
        sling_file_name = os.path.basename(sling_file)
        with open(sling_file, "r") as f:
            sling_config = yaml.load(f, Loader=yaml.FullLoader)  # Загружаем Ямл
            result_datasets[sling_file_name] = []
            for stream_name, stream_configs in sling_config.get("streams", {}).items():
                dataset_url = f"{target_conn.conn_type}://{target_conn.host}:{target_conn.port}/{target_conn.schema}/{stream_configs.get('object')}"
                result_datasets[sling_file_name].append(Dataset(dataset_url))


# Define the DAG
@dag(
    dag_id=DAG_ID,
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    doc_md=readme_content,
    tags=["sling", "minio", "clickhouse"],
    default_args={
        "owner": "ramis.khasianov",
        "retries": 0,
        "retry_delay": duration(minutes=1),
        "email_on_failure": True,
        "email": ["ramis.khasianov@raports.io"],
    },
)
def dag():
    # Since we are using a ClickHouse cluster, we need to create the database and tables on all nodes
    create_database = ClickHouseOperator(
        task_id="create_database",
        database="default",
        clickhouse_conn_id=target_conn.conn_id,
        sql="""
            create database if not exists app_metrica on cluster default
        """,
    )

    # Create local and later distributed tables
    create_local_table = ClickHouseOperator(
        task_id="create_local_table",
        database="default",
        clickhouse_conn_id=target_conn.conn_id,
        sql="""
            create table if not exists app_metrica.raw_usage_metrics_local on cluster default (
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
            engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/app_metrica/raw_usage_metrics_local', '{replica}')
            partition by `date`
            order by (`os_name`, `date`)
        """,
    )

    # Create distributed table
    create_distributed_table = ClickHouseOperator(
        task_id="create_distributed_table",
        database="default",
        clickhouse_conn_id=target_conn.conn_id,
        sql="""
            create table if not exists app_metrica.raw_usage_metrics on cluster default
            as app_metrica.raw_usage_metrics_local
            engine = Distributed(default, app_metrica, raw_usage_metrics_local, cityHash64(os_name));
        """,
    )

    create_database >> create_local_table >> create_distributed_table

    # Sling tasks to load data from MinIO to ClickHouse. Each sling*.yaml file corresponds to one task group
    with TaskGroup(group_id="sling_from_minio_raports_io") as task_group:

        # Get MinIO connection
        source_conn = BaseHook.get_connection(conn_id="minio_raports_io")
        # Since we can have multiple sling files, we create one BashOperator per sling file. Name of the file corresponds to task group's group_id
        sling_file_name = f"{task_group.group_id}.yaml"  # sling_from_minio_raports_io.yaml

        run_sling = BashOperator(
            task_id="run_sling",
            outlets=result_datasets[sling_file_name],
            bash_command=f"sling run -r {DAG_FOLDER_PATH}/{sling_file_name} -d",
            env={
                "PATH": "/home/airflow/.local/bin",
                "minio_raports_io__app_metrica": f"""{{
                    type: s3, 
                    bucket: 'app-metrica', 
                    access_key_id: {source_conn.login}, 
                    secret_access_key: "{source_conn.password}", 
                    endpoint: {json.loads(source_conn.extra).get("endpoint_url", "") if source_conn.extra else ""}
                }}""",
                "clickhouse_raports_io": f"clickhouse://{target_conn.login}:{target_conn.password}@{target_conn.host}:{target_conn.port}/{target_conn.schema}",
            },
        )

        create_distributed_table >> run_sling


dag_instance = dag()
