
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner' : 'intellipaat',
    'depends_on_past':'False',
    'start_date': datetime(2025,2,15),
    'end_date': datetime(2026,2,15),
    'retries':2,
    'retry_delay':timedelta(minutes=3),
    'execution_timeout':timedelta(minutes=60)
    }


dag = DAG(    
    '01_simple_dag',
    default_args=default_args,
    description='Simple dag',
    schedule_interval="*/5 * * * *",  # Corrected schedule interval
    # schedule_interval=timedelta(minutes=5)
    )


# Task 1 -  PythonOperator
def hello():
    print('Hello from simple Dag')

task1 = PythonOperator(
    task_id = 'python_task_1',
    python_callable=hello,
    dag=dag
    )

# Task 2 -  BashOperator
task2 = BashOperator(
    task_id='bash_task_1',
    bash_command='ls -l',
    dag=dag
    )

task1>>task2
