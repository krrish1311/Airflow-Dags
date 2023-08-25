from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import subprocess
local_tz = timezone('Asia/Kolkata') 
def write_date():
    file=open('/home/krrish/py_date.txt','a+')
    file.write(subprocess.getoutput('date')+'\n')
    file.close()
default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,4,25)),
    'retries': 1,
    'retry_delay': timedelta(days=1),
}


with DAG(
    dag_id='dag-sleep_3',
    default_args=default_args,
    description='My third DAG',
    schedule_interval=timedelta(minutes=5),
) as dag:
     task1=BashOperator(
        task_id='task01',
        bash_command='sleep 600'
    )
task1
