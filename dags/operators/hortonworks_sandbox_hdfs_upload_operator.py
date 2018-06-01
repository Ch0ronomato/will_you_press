from airflow.operators import BaseOperator
from os import path
from requests import put
from uuid import uuid1

class HortonworksSandboxHDFSUploadOperator(BaseOperator):
    HORTONWORKS_SANDBOX_UPLOAD_URL = "http://localhost:8080/api/v1/views/FILES/versions/1.0.0/instances/AUTO_FILES_INSTANCE/resources/files/upload"
    def __init__(self, parent_task='', file_name='', file_path='/user/maria_dev/', *args, **kwargs):
        super(HortonworksSandboxHDFSUploadOperator, self).__init__(*args, **kwargs)
        self.parent_task = parent_task
        self.file_name = file_name
        self.file_path = file_path

    def execute(self, context):
        content, uuid = context['task_instance'].xcom_pull(task_ids=[self.parent_task, 'get_uuid_for_folder'])
        execution_date = context['execution_date'].strftime('%Y_%m_%d')
        file_path = path.join(self.file_path, execution_date, str(uuid))
        self._push_file_to_hdfs(file_path, self.file_name, content)
        

    def _push_file_to_hdfs(self, hdfs_path, file_name, file_content):
        resp = put(self.HORTONWORKS_SANDBOX_UPLOAD_URL, 
            auth=('maria_dev', 'maria_dev'), 
            files={
                'path': hdfs_path, 
                'file': (file_name, file_content)
            }, 
            headers={'X-Requested-By': "maria_dev"}
        ) 
        resp.raise_for_status()
