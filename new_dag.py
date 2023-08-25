from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

local_tz = timezone('Asia/Kolkata') 
import smtplib
from email.message import EmailMessage

def send_mail_onfail():
    smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
    smtp_server.starttls()
    smtp_server.login('xrrishdummy@gmail.com', 'fapizjbrcbhnkhfi')

    message = EmailMessage()
    message['From'] = 'xrrishdummy@gmail.com'
    message['To'] = ['krishgoal2000@gmail.com','krishnakant.digole@iauro.com']
    message['Subject'] = '! Dag failing ALert !!'
    message.set_content('The dag_id  has been failed')

    # smtp_server.send_message(message, message['From'], message['To'])
    smtp_server.send_message(message)

    smtp_server.quit()

def test_fun():
     print(0/0)

def on_failure_callback(context):
     print('mail sending started')

     send_mail_onfail()
     print('mail has been send')



default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,6,23)),
    'retries': 1,
    "email_on_failure": True,
    'retry_delay': timedelta(seconds=10),
    "on_failure_callback": on_failure_callback
}

with DAG(
    dag_id='check_on_fail_call',
    default_args=default_args,
    description='test',
    schedule_interval=timedelta(days=1),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        python_callable=test_fun,
        
    )
     task1