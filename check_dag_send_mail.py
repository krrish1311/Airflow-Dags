from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
local_tz = timezone('Asia/Kolkata') 
email_body=''
def dag_runs(dag_id,state,limit=100):
    dag_run_resp=requests.get('http://localhost:8080/api/v1/dags/{}/dagRuns?limit={}&state={}'.format(dag_id,limit,state),auth=('Admin', '7A6WrqcdpK8bA66A'))
    dag_run_output=dag_run_resp.json()
    total_dag_run=dag_run_output['total_entries']
    return total_dag_run
    
def checking_dags_state(**context):
    over_dags={}
    context['task_instance'].xcom_push(key='email_flag', value=False)

    response_dags=requests.get('http://localhost:8080/api/v1/dags',auth=('Admin', '7A6WrqcdpK8bA66A'))
    output=response_dags.json()
    all_dags=output['dags']
    for dag in all_dags:
        dags_count=dag_runs(dag_id=dag['dag_id'],state='running')
        print(f"the dag_id {dag['dag_id']} has {dags_count} running")
        if dags_count>=2:
            context['task_instance'].xcom_push(key='email_flag', value=True)

            over_dags[dag['dag_id']]=dags_count
            # return dag['dag_id'],dags_count
    context['task_instance'].xcom_push(key='dags_over', value=over_dags)

def future_tasks(**context):
    success=context['task_instance'].xcom_pull(key='email_flag', task_ids='task01')
    over_dags=context['task_instance'].xcom_pull(key='dags_over', task_ids='task01')

    print(success)
    print(over_dags)
    if success:
        return 'task02'
    else:
        return 'task03'

default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,4,20,17,27)),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='all_check_dag_status_send_mail',
    default_args=default_args,
    description='My third DAG',
    schedule_interval=timedelta(days=1),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        python_callable=checking_dags_state,
        provide_context=True,
        
        
    )
     branch_task=BranchPythonOperator(
         task_id='branch',
         provide_context=True,
         trigger_rule='all_success',
         python_callable=future_tasks
     )
     task2=EmailOperator(
        task_id='task02',
        to='Krishgoal2000@gmail.com',
        subject='Airflow DAGs Statuses!!',
        html_content="<h3>the following dag {{ task_instance.xcom_pull(key='dags_over',task_ids='task01')}} </h3>",

     )
     task3=BashOperator(
        task_id='task03',
        bash_command="echo 'There is no tasks are running that much'"
    )
     task1>>branch_task>>[task2,task3]
