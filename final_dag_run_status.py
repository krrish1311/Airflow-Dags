
import smtplib
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
local_tz = timezone('Asia/Kolkata') 



def get_dag_run_info(dag_id,order_by,limit=100):
    dag_run_resp=requests.get('http://localhost:8080/api/v1/dags/{}/dagRuns?limit={}&order_by={}'.format(dag_id,limit,order_by),auth=('Admin', '7A6WrqcdpK8bA66A'))
    dag_run_output=dag_run_resp.json()
    if dag_run_output['total_entries']>0:
        return dag_run_output
    else:
        return False

def sort_dag_running(dag_run_resp,dag_id):
    sorted_dag_runs=[]
    start_date=0
    start_date_obj=0
    total_running_dagruns=0
    for dag_run in dag_run_resp:
        if dag_run['state']=='failed' or dag_run['state']=='queued':
            pass
        elif dag_run['state']=='running':
            temp_date_obj=datetime.fromisoformat(dag_run['start_date'])
            if start_date==0 or start_date_obj>temp_date_obj:
                start_date=dag_run['start_date']
                start_date_obj=temp_date_obj

            total_running_dagruns+=1
            
                
#            sorted_dag_runs.append(dag_run)
        else:
            return start_date,total_running_dagruns
    return start_date,total_running_dagruns    
        

def check_running_dag(dag_id):
    total_task_instances=0
    all_dag_runs=get_dag_run_info(dag_id,'state')
#     print(all_dag_runs)
    if all_dag_runs:
        task_instances=sort_dag_running(all_dag_runs['dag_runs'],dag_id)
        print("output",task_instances)
        if task_instances[0]!=0 and task_instances[1]>5:
            dag_info={'DAG Name':dag_id,'Total Running Instances':task_instances[1],'Stuck Since':task_instances[0],'Latest DAG Status':'running'}
            return dag_info
        else:
            return 'no_running'

    else:
            return 'no_running'    


def sort_dag_failed(dag_run_resp):
    sorted_dag_run=None
    start_date=0
    start_date_obj=0
    for dag_run in dag_run_resp:
#         print(dag_run)
        if dag_run['state']=='failed':
            temp_date_obj=datetime.fromisoformat(dag_run['start_date'])
            if start_date==0 or start_date_obj<temp_date_obj:
                start_date=dag_run['start_date']
                start_date_obj=temp_date_obj
                sorted_dag_run=dag_run
            
                       
           
        else:
            return sorted_dag_run
        

def check_failling_dag(dag_id):
    all_dag_runs=get_dag_run_info(dag_id,'state')
    if all_dag_runs:
        failed_dag_run=sort_dag_failed(all_dag_runs['dag_runs'])
        if failed_dag_run!=None:
            failed_dag_info={'DAG Name':dag_id,'Failed Since':failed_dag_run['end_date'],'Latest DAG Status':'failed'} 
            return failed_dag_info 
        else:
            return 'no_failed'      

    
    else:
        return 'no_failed'

def check_failling_dag(dag_id):
    all_dag_runs=get_dag_run_info(dag_id,'-start_date',limit=1)
    if all_dag_runs:
        # failed_dag_run=sort_dag_failed(all_dag_runs['dag_runs'])
        dag_run_status=all_dag_runs['dag_runs'][0]['state']
        if dag_run_status=='failed':
            failed_dag_info={'DAG Name':dag_id,'Failed Since':failed_dag_run['end_date'],'Latest DAG Status':'failed'} 
            return failed_dag_info 
        else:
            return 'no_failed'      

    
    else:
        return 'no_failed'          
    

def email_body_table(dag_info,email_body):
#         email_body="<table><thead><tr>"
        for key in dag_info[0].keys():
            email_body=email_body+'<th>'+key+'</th>'
        email_body=email_body+'</tr></thead><tbody>'
        for i in dag_info:
            email_body=email_body+'<tr>'
            for j in i:
                email_body=email_body+'<td align=center>'+str(i[j])+'</td>'
            email_body=email_body+'</tr>'
        email_body=email_body+'</tbody></table>' 
        return email_body

                
def send_mail(subject,email_body):
    smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
    smtp_server.starttls()
    smtp_server.login('xrrishdummy@gmail.com', 'fapizjbrcbhnkhfi')
    
    message = MIMEMultipart()
    
    body = MIMEText(email_body, 'html')
    message.attach(body)

    
#     message = EmailMessage()
    message['From'] = 'xrrishdummy@gmail.com'
    # message['To'] = 'mayur.deshpande@iauro.com'
    message['To'] = 'krishgoal2000@gmail.com'
    message['Subject'] = subject

#     smtp_server.send_message(message, 'xrrishdummy@gmail.com', ' mayur.deshpande@iauro.com')
    smtp_server.sendmail(message['From'], message['To'], message.as_string())

    smtp_server.quit()
        


def all_dag_fail_run_status():
    all_running_dag=[]
    all_failed_dag=[]
    response_dags=requests.get('http://localhost:8080/api/v1/dags?limit=100&only_active=true',auth=('Admin', '7A6WrqcdpK8bA66A'))
    output=response_dags.json()
    all_dags=output['dags']
    # send_running_mail=False
    # send_failed_mail=False
    for dag in all_dags:
        dag_run_info=check_running_dag(dag['dag_id'])
        if dag_run_info=='no_running':
            pass
        else:
            all_running_dag.append(dag_run_info)
        dag_failed_info=check_failling_dag(dag['dag_id'])
        if dag_failed_info=='no_failed':
            pass
        else:
            all_failed_dag.append(dag_failed_info)
    print(all_running_dag)
    print(all_failed_dag)
    if len(all_running_dag)>0:
        running_body=email_body_table(dag_info=all_running_dag,email_body="<table border=2 ><thead><caption>DAG's with Multiple Running Instances</caption><tr>")
        if len(all_failed_dag)>0:
            email_body=email_body_table(dag_info=all_failed_dag,email_body=running_body+"</br></br></br></br></br></br><table border=2 ><thead><caption>DAG's in Error State</caption><tr>")
            print(email_body)
            send_mail("!!DAG's STATUS",email_body=email_body)
        else:
            running_body=running_body+"</br></br></br></br></br></br><h2>Failed Dag's are Not Found!</h2>" 
            send_mail("!!DAG's STATUS",email_body=running_body)
            
    elif len(all_failed_dag)>0:
        failed_body=email_body_table(dag_info=all_failed_dag,email_body="<h2>No Running Dag's are present</h2>"+"</br></br></br></br></br></br><table border=2 ><thead><caption>DAG's in Error State</caption><tr>")
        send_mail("!!DAG's STATUS",email_body=failed_body)
                        
    else:
        print('no Running and Failed dags are founded')                    

default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,6,1)),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'catchup': False,

}

with DAG(
    dag_id='DAG_STATUS_run_fail_3',
    default_args=default_args,
    description="Checking all DAG's status",
    schedule_interval=timedelta(minutes=30),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        python_callable=all_dag_fail_run_status,
        
    )
task1 
