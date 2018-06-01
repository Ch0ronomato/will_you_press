import requests
from airflow.operators import BaseOperator
from datetime import datetime

class ParseDilemmaContentOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super(ParseDilemmaContentOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        raise(context.xcom_pull(task_ids=[
            'download_dilemma_content',
            'download_dilemma_yeses'
        ]))
