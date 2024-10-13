from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior

# adjust for other database types
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime
import os

DAG_ID = "jaffle_shop_pipeline"
CONNECTION_ID = "dwh"
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/repo/dags/dbt/jaffle-shop"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"


profile_config = ProfileConfig(
    profile_name="dwh",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dwh",
        profile_args={"schema": 'public'},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
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
    # run_airbyte = Ai

    dbt_run = DbtTaskGroup(
        group_id="dbt_run",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={
            "install_deps": True
        }
    )

    from cosmos.operators import DbtDocsS3Operator

    generate_dbt_docs_s3 = DbtDocsS3Operator(
        task_id="generate_dbt_docs_s3",
        profile_config=profile_config,
        project_dir=DBT_PROJECT_PATH,
        connection_id="minio",
        bucket_name="dbt-docs",
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        folder_dir=DAG_ID,
        operator_args={
            "install_deps": True
        }
    )

    dbt_run >> generate_dbt_docs_s3

dag()