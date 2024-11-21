from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime


@dag(schedule_interval='@daily', start_date=datetime(2022, 1, 1), catchup=False)
def app_usage_metrics():
    sling_task = BashOperator(
        task_id='run_sling',
        bash_command='sling conns list',
    )

app_usage_metrics()