from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior

# adjust for other database types
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime
import os

DAG_ID = "jaffle_shop"

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/repo/dags/jaffle_shop/dbt/jaffle-shop"
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
        connection_id='50c7a352-0c75-4208-ba43-267bf6941d7b',
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

    from cosmos.operators import DbtDocsS3Operator

    generate_dbt_docs_to_s3 = DbtDocsS3Operator(
        task_id="generate_dbt_docs_to_s3",
        profile_config=profile_config,
        project_dir=DBT_PROJECT_PATH,
        connection_id="default_minio.raports.net",
        bucket_name="dbt",
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        folder_dir=DAG_ID,
        install_deps=True
    )

    run_airbyte >> run_dbt >> generate_dbt_docs_to_s3

dag()