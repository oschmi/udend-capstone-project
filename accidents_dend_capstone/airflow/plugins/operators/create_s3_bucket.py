from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateS3BucketOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_connection_id,
                 bucket_name,
                 region_name,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_connection_id = aws_connection_id
        self.bucket_name = bucket_name
        self.region_name = region_name

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_connection_id)
        self.log.info(f"Trying to create bucket <{self.bucket_name}> in region <{self.region_name}>")
        if s3_hook.check_for_bucket(self.bucket_name):
            self.log.info(f"Bucket <{self.bucket_name}> already exists! Doing nothing...")
            return
        s3_hook.create_bucket(bucket_name=self.bucket_name, region_name=self.region_name)
        self.log.info(f"Successfully created bucket <{self.bucket_name}>")
