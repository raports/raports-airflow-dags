import os

from airflow import Dataset
from pendulum import datetime, duration

from airflow.decorators import dag

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.operators import DbtDocsS3Operator


DAG_ID = "jaffle_shop__t"

README_FILE_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/repo/dags/{DAG_ID}/README.md"

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/repo/dags/{DAG_ID}/dbt_project"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dwh",
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dwh_postgresql.raports.net",
        profile_args={"schema": 'public'},
    ),
)

with open(README_FILE_PATH, "r") as readme_file:
    readme_content = readme_file.read()

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 10, 12),
    schedule=(
            Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_customers')
            | Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_items')
            | Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_orders')
            | Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_products')
            | Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_stores')
            | Dataset('postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_supplies')
    ),
    catchup=False,
    doc_md=readme_content,
    tags=['dbt', 'postgresql'],
    max_active_tasks=3,
    default_args={
        'owner': 'ramis.khasianov',
        'retries': 1,
        'retry_delay': duration(minutes=1),
        'email_on_failure': True,
        'email': ['ramis.khasianov@raports.net']
    }
)
def dag():

    run_dbt = DbtTaskGroup(
        group_id="run_dbt",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(
            test_behavior=TestBehavior.AFTER_ALL,
        ),
        operator_args={
            "install_deps": True
        }
    )


    generate_dbt_docs_to_s3 = DbtDocsS3Operator(
        task_id="generate_dbt_docs_to_s3",
        profile_config=profile_config,
        project_dir=DBT_PROJECT_PATH,
        connection_id="default_minio.raports.net",
        bucket_name="dbt-docs",
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        folder_dir=DAG_ID,
        install_deps=True
    )

    run_dbt >> generate_dbt_docs_to_s3

dag()