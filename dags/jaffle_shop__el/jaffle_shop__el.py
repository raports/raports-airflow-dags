import os
from pendulum import datetime, duration

from airflow import Dataset
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator


DAG_ID = "jaffle_shop__t"

README_FILE_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/repo/dags/{DAG_ID}/README.md"

with open(README_FILE_PATH, "r") as readme_file:
    readme_content = readme_file.read()

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 10, 12),
    schedule=None,
    catchup=False,
    doc_md=readme_content,
    tags=['airbyte', 'minio', 'postgresql'],
    default_args={
        'owner': 'ramis.khasianov',
        'email_on_failure': True,
        'email': ['ramis.khasianov@raports.net']
    }
)
def dag():

    run_airbyte = AirbyteTriggerSyncOperator(
        task_id='run_airbyte',
        airbyte_conn_id='default_airbyte.raports.net',
        connection_id=Variable.get('airbyte__jaffle_shop'),
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
        outlets=[
            Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_customers'),
            Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_items'),
            Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_orders'),
            Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_products'),
            Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_stores'),
            Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_supplies'),
        ]
    )

dag()