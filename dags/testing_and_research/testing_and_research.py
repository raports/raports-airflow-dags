from pendulum import datetime
from airflow.decorators import dag, task

DAG_ID = 'testing_and_research'

@dag(
    dag_id=DAG_ID,
    schedule_interval=None,
    start_date=datetime(2025, 4, 1),
    catchup=False,
    default_args={
        'owner': 'ramis.khasianov'
    }
)
def dag():

    @task
    def start(ds=None):
        print(ds)
        return 42

    @task
    def print_val(value: int):
        print(value)

    print_val(start())

dag()