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

def printname(ti):
    fname = ti.xcom_pull(task_ids='t2', key='fname')
    lname = ti.xcom_pull(task_ids='t2', key='lname')
    age = ti.xcom_pull(task_ids='t3')
    print(f'Hi my name is {fname} {lname} and age is {age}')

def setname(ti):
    ti.xcom_push(key='fname', value='Zainab')
    ti.xcom_push(key='lname', value='Badshah')

def setage():
    age=26
    return age

with DAG("variabledemo_v4",
        default_args=default_args,
        schedule_interval='@daily',
):
    task1 = PythonOperator(task_id = 't1',
                            python_callable = printname)
    task2 = PythonOperator(task_id = 't2',
                           python_callable = setname)
    task3 = PythonOperator(task_id = 't3',
                           python_callable = setage)



task1 << [task2,task3]