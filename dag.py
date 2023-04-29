# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'GF',
    'start_date': days_ago(0),
    'email':"somemail@mail.com"
}

# define the DAG
dag = DAG(
    dag_id='process_web_log',
    default_args=default_args,
    description='web log',
    schedule_interval=timedelta(minutes=1),
)

extract = BashOperator(
    task_id='extract_data',
    bash_command='cut -d" " -f1 /home/project/airflow/dags/accesslog.txt > /home/project/airflow/dags/extracted-data.txt',
    dag=dag,
)

transform = BashOperator(
    task_id='transform_data',
    bash_command="sed '/198\.46\.149\.143/d' /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed.txt",
    dag=dag,
)

#
load = BashOperator(
    task_id='load_data',
    bash_command="tar -cvf /home/project/airflow/dags/weblog.tar /home/project/airflow/dags/transformed.txt",
    dag=dag,
)

## define the pipeline
extract >> transform >> load