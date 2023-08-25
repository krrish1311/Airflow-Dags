from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
import random 
local_tz = timezone('Asia/Kolkata')

a=range(10)
def success_or_fail():
    if random.choice(a)%2==0:
        2+2
    else:
        1/0

def future_tasks(**context):
    success=context['ti'].state
    print(success)
    if success=='success':
        return 'task02'
    else:
        return 'task03'
    
def task_2():
    print("The above task has been failed so...")

def task_3():
    print("The above task has been failed so...")

default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,4,21,11,52)),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='branching_tasks',
    default_args=default_args,
    description='My third DAG',
    schedule_interval=timedelta(days=1),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        python_callable=success_or_fail
    )
     branch_task=BranchPythonOperator(
         task_id='branch',
         provide_context=True,
         trigger_rule='all_done',
         python_callable=future_tasks
     )
     task2=PythonOperator(
         task_id='task02',
         python_callable=task_2

     )
     task3=PythonOperator(
         task_id='task03',
         python_callable=task_3

     )
     task1>>branch_task>>[task2,task3]
