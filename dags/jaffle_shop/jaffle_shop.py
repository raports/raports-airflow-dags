from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior

# adjust for other database types
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.operators import DbtDocsS3Operator
from pendulum import datetime
import os

DAG_ID = "dbt_jaffle_shop"

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/repo/dags/{DAG_ID}/dbt/dbt_{DAG_ID}"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"


profile_config = ProfileConfig(
    profile_name="dwh",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dwh_postgresql.raports.net",
        profile_args={"schema": 'public'},
    ),
)

render_config=RenderConfig(
    test_behavior=TestBehavior.AFTER_ALL,
)

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 10, 12),
    schedule=None,
    catchup=False,
)
def dag():

    run_airbyte = AirbyteTriggerSyncOperator(
        task_id='run_airbyte',
        airbyte_conn_id='default_airbyte.raports.net',
        connection_id=Variable.get('airflow-jaffle-shop'),
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

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

    run_airbyte >> run_dbt >> generate_dbt_docs_to_s3

dag()