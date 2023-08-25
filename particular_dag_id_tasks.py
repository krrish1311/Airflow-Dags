from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
import requests
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
local_tz = timezone('Asia/Kolkata') 
email_body=''
def task_instances(dag_id,dag_run_id,state,limit=100):
    task_inst=requests.get('http://localhost:8080/api/v1/dags/{}/dagRuns/{}/taskInstances?limit={}&state={}'.format(dag_id,dag_run_id,limit,state),auth=('Admin', '7A6WrqcdpK8bA66A'))
    task_inst_resp=task_inst.json()
    total_task=task_inst_resp['total_entries']
    print(f"in {dag_id} dag_id the dag_run_id {dag_run_id} has {total_task} running tasks")
    
    return total_task
    
def dag_runs_2(dag_id,state,limit=100):
    dag_run_resp=requests.get('http://localhost:8080/api/v1/dags/{}/dagRuns?limit={}&state={}'.format(dag_id,limit,state),auth=('Admin', '7A6WrqcdpK8bA66A'))
    dag_run_output=dag_run_resp.json()
    total_dag_run=dag_run_output['total_entries']
#     return total_dag_run
    total_instances_dag=0
    for dag_run in dag_run_output['dag_runs']:
        total_tasks_dag_run=task_instances(dag_id,dag_run['dag_run_id'],'running')
        total_instances_dag=total_tasks_dag_run+total_instances_dag

        # if total_tasks>=2:
        #     return dag_id,dag_run['dag_run_id'],total_tasks

    return total_instances_dag,dag_id    

        
        
    
    
def checking_dags_state(**context):
        context['task_instance'].xcom_push(key='email_flag', value=False)
        dags_inst_count=dag_runs_2(dag_id='dag-sleep_2',state='running')
        
        if dags_inst_count[0]>=2:
            context['task_instance'].xcom_push(key='email_flag', value=True)
            # return dags_inst_count
            context['task_instance'].xcom_push(key='tasks_over', value=dags_inst_count)


def future_tasks(**context):
    success=context['task_instance'].xcom_pull(key='email_flag', task_ids='task01')
    over_dags=context['task_instance'].xcom_pull(key='tasks_over', task_ids='task01')

    print(success)
    print(over_dags)
    if success:
        return 'task02'
    else:
        return 'task03'


default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,4,20,17,27)),
    'depends_on_past': False,
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='particular_dag_task_instance_send_mail',
    default_args=default_args,
    description='My third DAG',
    schedule_interval=timedelta(days=1),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        provide_context=True,
        python_callable=checking_dags_state,
        
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
        subject='!!!Airflow DAG instances Status!!!',
        html_content="<h3>the following dag {{ task_instance.xcom_pull(key='tasks_over',task_ids='task01')[1] }} has {{ task_instance.xcom_pull(key='tasks_over',task_ids='task01')[0] }} task _instances running </h3>",

     )
     task3=BashOperator(
        task_id='task03',
        bash_command="echo 'There is no tasks are running that much'"
    )
     task1>>branch_task>>[task2,task3]
