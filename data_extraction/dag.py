from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 2, 10),
    'depends_on_past': False,
    'email': ['dayasagarreddy1@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
}

dag = DAG('fetch_cricket_stats',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='run_script',
        bash_command='python /home/airflow/gcs/dags/scripts/pushtoGCS.py', 
        #bash_command='us-central1-test-dag-8eaa8422-bucket/dags/scripts/pushtoGCS.py',
    )