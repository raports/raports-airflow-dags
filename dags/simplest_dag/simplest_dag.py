from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule_interval='@daily', start_date=datetime(2022, 1, 1), catchup=False)
def simplest_dag():

    @task
    def start(ds=None):
        print(ds)
        return 42

    @task
    def print_val(value: int):
        print(value)

    print_val(start())

simplest_dag()