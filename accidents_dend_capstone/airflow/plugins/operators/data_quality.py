from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


class DataQualityOperator(BaseOperator):
    ui_color = '#B2CCFF'

    @apply_defaults
    def __init__(self,
                 aws_connection_id,
                 bucket_name,
                 prefix="",
                 expected_count=0,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_connection_id = aws_connection_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.expected_count = expected_count

    def execute(self, context):
        s3_hook = S3Hook(self.aws_connection_id)
        keys = s3_hook.list_keys(self.bucket_name, prefix=self.prefix)

        if len(keys) != self.expected_count:
            raise AssertionError(f'S3 file count doesnt match: {len(keys)} != {self.expected_count}')
