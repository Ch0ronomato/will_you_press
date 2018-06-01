from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime, timedelta
from operators.hortonworks_sandbox_hdfs_upload_operator import HortonworksSandboxHDFSUploadOperator
from operators.parse_dilemma_content_operator import ParseDilemmaContentOperator
import requests
from uuid import uuid1

def map_to_dilemma_id(**context):
    start_date = datetime(2000, 1, 1)
    return (context['execution_date'] - start_date).days

def get_dilemma_content(**context):
    request_url = "http://www.willyoupressthebutton.com/{request_id}"
    request_id = context['task_instance'].xcom_pull(task_ids='get_dilemma_id')
    dilemma_content = requests.get(request_url.format(request_id=request_id))
    return dilemma_content.text

def get_dilemma_yeses(**context):
    request_url = "http://www.willyoupressthebutton.com/{request_id}/stats/yes"
    request_id = context['task_instance'].xcom_pull(task_ids='get_dilemma_id')
    dilemma_content = requests.get(request_url.format(request_id=request_id))
    return dilemma_content.text

def get_uuid_for_folder(**context):
    return uuid1()

WORKFLOW_DAG_ID = "will_you_press_the_button"

WORKFLOW_START_DATE = datetime(2018, 5, 26)

WORKFLOW_SCHEDULE_INTERVAL = timedelta(days=1)

WORKFLOW_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False, 
    'start_date': WORKFLOW_START_DATE,
    'email': ['schweerian33@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    WORKFLOW_DAG_ID,
    start_date=WORKFLOW_START_DATE,
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    default_args=WORKFLOW_DEFAULT_ARGS
)

get_request_id = PythonOperator(
    task_id='get_dilemma_id',
    python_callable=map_to_dilemma_id,
    provide_context=True,
    dag=dag
)

get_dilemma_content = PythonOperator(
    task_id='get_dilemma_content',
    python_callable=get_dilemma_content,
    provide_context=True,
    dag=dag
)

get_dilemma_yeses = PythonOperator(
    task_id='get_dilemma_yeses',
    python_callable=get_dilemma_yeses,
    provide_context=True,
    dag=dag
)

get_uuid_for_folder = PythonOperator(
    task_id='get_uuid_for_folder',
    python_callable=get_uuid_for_folder,
    provide_context=True,
    dag=dag
)

put_dilemma_content_to_hdfs = HortonworksSandboxHDFSUploadOperator(
    task_id='put_dilemma_content_to_hdfs',
    dag=dag,
    parent_task='get_dilemma_content',
    file_name='content.html',
)

put_dilemma_stats_to_hdfs = HortonworksSandboxHDFSUploadOperator(
    task_id='put_dilemma_stats_to_hdfs',
    dag=dag,
    parent_task='get_dilemma_yeses',
    file_name='yeses.html',
)
get_request_id >> get_dilemma_content >> put_dilemma_content_to_hdfs 
get_request_id >> get_dilemma_yeses >> put_dilemma_stats_to_hdfs
get_uuid_for_folder >> put_dilemma_content_to_hdfs
get_uuid_for_folder >> put_dilemma_stats_to_hdfs
