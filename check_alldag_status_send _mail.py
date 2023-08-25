from datetime import datetime,timedelta
from pytz import timezone,utc
from airflow import DAG
import requests
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
local_tz = timezone('Asia/Kolkata') 
import smtplib
from email.message import EmailMessage

currentdate=datetime.now(tz=utc)-timedelta(hours=6)
start_date_gte=currentdate.strftime('%Y-%m-%dT%H:%M:%SZ')

def task_instances(dag_id,dag_run_id,state,limit=100):
    task_inst=requests.get('http://localhost:8080/api/v1/dags/{}/dagRuns/{}/taskInstances?limit={}&state={}'.format(dag_id,dag_run_id,limit,state),auth=('Admin', '7A6WrqcdpK8bA66A'))
    task_inst_resp=task_inst.json()
    total_task=task_inst_resp['total_entries']
    print(f"in {dag_id} dag_id the dag_run_id {dag_run_id} has {total_task} running tasks")
    
    return total_task
    

def send_mail_onfail(dag_id):
    smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
    smtp_server.starttls()
    smtp_server.login('xrrishdummy@gmail.com', 'fapizjbrcbhnkhfi')

    message = EmailMessage()
    message['From'] = 'xrrishdummy@gmail.com'
    message['To'] = 'krishgoal2000@gmail.com'
    message['Subject'] = '! Dag failing ALert !!'
    message.set_content('The dag_id {} has been failed'.format(dag_id))

    smtp_server.send_message(message, 'xrrishdummy@gmail.com', 'krishgoal2000@gmail.com')

    smtp_server.quit()


def send_mail_onrunning(dag_id,total_running):
    smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
    smtp_server.starttls()
    smtp_server.login('xrrishdummy@gmail.com', 'fapizjbrcbhnkhfi')

    message = EmailMessage()
    message['From'] = 'xrrishdummy@gmail.com'
    message['To'] = 'krishgoal2000@gmail.com'
    message['Subject'] = 'Dag Running ALert !!'
    message.set_content('The dag_id {} has total {} running tasks'.format(dag_id,total_running))

    smtp_server.send_message(message, 'xrrishdummy@gmail.com', 'krishgoal2000@gmail.com')

    smtp_server.quit()


def check_dag_status():
    response_dags=requests.get('http://localhost:8080/api/v1/dags?limit=100&only_active=true',auth=('Admin', '7A6WrqcdpK8bA66A'))
    output=response_dags.json()
    all_dags=output['dags']
    for dag in all_dags:
        dag_run_resp=requests.get('http://localhost:8080/api/v1/dags/{}/dagRuns?limit={}&start_date_gte={}&state={}&state={}'.format(dag['dag_id'],100,start_date_gte,'running','failed'),auth=('Admin', '7A6WrqcdpK8bA66A'))
        all_dag_runs=dag_run_resp.json()
        dag_tasks_run_count=0
        dag_run_failed=0
        for dag_run in all_dag_runs['dag_runs']:
            if dag_run['state']=='failed':
                dag_run_failed+=1
                print('{} has failed'.format(dag['dag_id']))
            elif dag_run['state']=='running':
                dag_runs_tasks=task_instances(dag['dag_id'],dag_run['dag_run_id'],'running')
                dag_tasks_run_count+=dag_runs_tasks
        
        if dag_run_failed>0:
            send_mail_onfail(dag['dag_id'])
        if dag_tasks_run_count>=2:
            send_mail_onrunning(dag['dag_id'],dag_tasks_run_count)        



default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,5,8)),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'catchup': False,

}

with DAG(
    dag_id='check_dag_fail_run',
    default_args=default_args,
    description='My third DAG',
    schedule_interval=timedelta(days=1),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        python_callable=check_dag_status,
        
    )
task1 
     




