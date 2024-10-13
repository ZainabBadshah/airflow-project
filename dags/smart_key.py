from random import randint
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 
import pandas as pd
import re

default_args = {
        'owner': "airflow",
        'start_date': datetime(2024,10,8,7),
        'retries': 2,
        'retry_delay': timedelta(seconds=5)
        }

def passphrase():
    df1= pd.read_csv('/opt/airflow/filestorage/customer_output.csv')

    def gen_code(fullname,phone):
        num = randint(1,99)
        val = str(phone)[-4:]
        passcode= str(num)+ fullname[1:2]+ str(val)
        return passcode
    
    df1['passphrase'] = df1.apply(lambda x: gen_code(x.full_name,x.phone), axis=1)
    df2 = pd.DataFrame(df1, columns=['full_name','passphrase'])
    df2.to_csv('/opt/airflow/filestorage/customer_output.csv', index=False)

def clean_name():
    df = pd.read_csv('/opt/airflow/filestorage/customer.csv')

    def remove_symbol(name):
        return re.sub(r'[^\w\s]','', name)
    
    df['last_name'] = df['last_name'].map(lambda x: remove_symbol(x))
    df['first_name'] = df['first_name'].map(lambda x: remove_symbol(x))

    df1 = pd.DataFrame(df, columns=['first_name','last_name','gender','phone'])
    df1.to_csv('/opt/airflow/filestorage/customer_output.csv', index=False)

def full_name():
    df1 = pd.read_csv('/opt/airflow/filestorage/customer_output.csv')

    def concat_name(fname, lname):
        fullname = fname + ' ' + lname
        return fullname
    df1['full_name'] = df1.apply(lambda x: concat_name(x.first_name, x.last_name), axis=1)
    df2 = pd.DataFrame(df1, columns=['full_name','gender','phone'])
    df2.to_csv('/opt/airflow/filestorage/customer_output.csv', index=False)

with DAG(
    'SmarttKeyData_v2',
    default_args=default_args,
    schedule_interval = '@daily',
):
    validate_task = BashOperator(task_id = 'Task1',
                                 bash_command = 'shasum /opt/airflow/filestorage/customer.csv')
    clean_task = PythonOperator(task_id = 'Task2',
                                python_callable= clean_name)
    gen_fullname = PythonOperator(task_id = 'Task3',
                                  python_callable= full_name)
    gen_code = PythonOperator(task_id= 'Task4',
                              python_callable = passphrase)
    

validate_task >> clean_task >> gen_fullname >> gen_code
    

    
    