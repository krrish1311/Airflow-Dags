from datetime import datetime,timedelta

from pytz import timezone

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.operators.email_operator import EmailOperator

local_tz = timezone('Asia/Kolkata')



def test_fun():

     print('hii')




default_args = {

    'owner': 'krrish',

    'start_date': local_tz.localize(datetime(2023,6,19)),

    'retries': 1,

    'retry_delay': timedelta(minutes=1),

}



with DAG(

    dag_id='checking_emailoperator_2',

    default_args=default_args,

    description='test',

    schedule_interval=timedelta(days=1),

) as dag:

     task1=PythonOperator(

        task_id='task01',

        python_callable=test_fun,

        

    )

     task2=EmailOperator(

        task_id='task02',

        to=['krishgoal2000@gmail.com'],

        subject='Checking Airflow EmailOperator',

        html_content="Hello World!",

        params={'email':"xrrishdummy@gmail.com" , 'password': "fapizjbrcbhnkhfi"}




     )

     task1>>task2