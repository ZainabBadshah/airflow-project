from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 


default_args = {
        'owner': "airflow",
        'start_date': datetime(2024,10,7,7),
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

def printtask():
    print("hello world")

with DAG("demo1_v1",
        default_args=default_args,
        schedule_interval='@daily',
):
    task1 = PythonOperator(task_id = 't1',
                            python_callable = printtask)
    task2 = BashOperator(task_id = 't2',
                         bash_command = "echo hello bash world")



task1 << task2