import os
from pendulum import datetime, duration

from airflow import Dataset
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator


DAG_ID = "jaffle_shop__el"

README_FILE_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/repo/dags/{DAG_ID}/README.md"

with open(README_FILE_PATH, "r") as readme_file:
    readme_content = readme_file.read()


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 10, 12),
    schedule=None,
    catchup=False,
    doc_md=readme_content,
    tags=["airbyte", "minio", "postgresql"],
    default_args={"owner": "ramis.khasianov", "email_on_failure": True, "email": ["ramis.khasianov@raports.io"]},
)
def dag():

    AirbyteTriggerSyncOperator(
        task_id="run_airbyte",
        airbyte_conn_id="airbyte_raports_io",
        connection_id="ea9065a2-2bcc-480f-a801-35f360909924",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
        outlets=[
            Dataset("postgres://postgresql.raports.io:5432/postgres.jaffle_shop.raw_customers"),
            Dataset("postgres://postgresql.raports.io:5432/postgres.jaffle_shop.raw_items"),
            Dataset("postgres://postgresql.raports.io:5432/postgres.jaffle_shop.raw_orders"),
            Dataset("postgres://postgresql.raports.io:5432/postgres.jaffle_shop.raw_products"),
            Dataset("postgres://postgresql.raports.io:5432/postgres.jaffle_shop.raw_stores"),
            Dataset("postgres://postgresql.raports.io:5432/postgres.jaffle_shop.raw_supplies"),
        ],
    )


dag()
