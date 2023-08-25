from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
local_tz = timezone('Asia/Kolkata') 
email_body=''
def checking_dags_state():
        response_dags=requests.get('http://localhost:8080/api/v1/dags',auth=('Admin', '7A6WrqcdpK8bA66A'))
        output=response_dags.json()
        for dags in output['dags'] :
            response_dr=requests.get('http://localhost:8080/api/v1/dags/{}/dagRuns'.format(dags['dag_id']),auth=('Admin', '7A6WrqcdpK8bA66A'))
            dag_runs=response_dr.json()
            running=0
        #     dag_status[dags['dag_id']]={'running':0,'pending':0,'success':0}
            for dag_run in dag_runs['dag_runs']:
                print(dag_run['state'])
                if dag_run['state']=='running':
                    running=running+1
        #             dag_status[dags['dag_id']]['running']+=1
        #             print(dag_run['state'])
            if running>=2:
                print(f"send mail {dags['dag_id']} dag has , {running} running instances")
                email_body=f"send mail {dags['dag_id']} dag has , {running} running instances"
                return  dags['dag_id'],running

default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,4,20,17,27)),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='send_mail_by_check',
    default_args=default_args,
    description='My third DAG',
    schedule_interval=timedelta(days=1),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        python_callable=checking_dags_state,
    )
     task2=EmailOperator(
        task_id='task02',
        to='Krishgoal2000@gmail.com',
        subject='Airflow DAG Status',
        html_content="<h3>the following dag {{ task_instance.xcom_pull(task_ids='task01')[0] }} has {{ task_instance.xcom_pull(task_ids='task01')[1] }} instances running </h3>",

     )
     task1>>task2