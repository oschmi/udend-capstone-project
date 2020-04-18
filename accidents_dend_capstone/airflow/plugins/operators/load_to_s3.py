import os

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pathlib import Path


class LoadFileToS3Operator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_connection_id,
                 bucket_name,
                 prefix=None,
                 from_path=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_connection_id = aws_connection_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.from_path = from_path

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_connection_id)

        if not os.path.exists(self.from_path):
            self.log.error(f"Cannot read from <{self.from_path}>: File or dir does not exist")
            raise FileNotFoundError(f"Cannot read from <{self.from_path}>: File or dir does not exist")

        if not s3_hook.check_for_bucket(self.bucket_name):
            self.log.error(f"Bucket <{self.bucket_name}> does not exists! Aborting file upload...")
            raise FileNotFoundError(f"Bucket <{self.bucket_name}> does not exists! Aborting file upload...")

        if os.path.isfile(self.from_path):
            self._handle_file(s3_hook)
            return

        if os.path.isdir(self.from_path):
            self._handle_dirs(s3_hook)

    def _handle_file(self, s3_hook):
        self.log.info(f"Uploading file <{self.from_path}>")
        if self.prefix is not None:
            key = self.prefix + "/" + os.path.basename(self.from_path)
            s3_hook.load_file(self.from_path, key, self.bucket_name, replace=True)
            self._log_success(self.from_path, key)
        else:
            key_name = os.path.basename(self.from_path)
            s3_hook.load_file(self.from_path, key_name, self.bucket_name, replace=True)
            self._log_success(self.from_path, key_name)

    def _handle_dirs(self, s3_hook):
        self.log.info(f"Uploading dir <{self.from_path}>")
        for root, dirs, files in os.walk(self.from_path):
            for file in files:
                full_path = Path(os.path.join(root, file))
                # avoid \ on windows in s3_key
                key = str(full_path.parent.name) + "/" + full_path.name
                s3_hook.load_file(filename=str(full_path), bucket_name=self.bucket_name, key=key, replace=True)
                self._log_success(full_path, key)

    def _log_success(self, full_path, key):
        self.log.info(f"Successfully loaded {full_path} to s3://{self.bucket_name}/{key}")
