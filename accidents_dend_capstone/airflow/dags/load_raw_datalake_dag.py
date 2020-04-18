from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import CreateS3BucketOperator, LoadFileToS3Operator, DataQualityOperator
from helpers import DefaultSettings

default_args = {
    'owner': 'oschmi',
    'start_date': datetime(2020, 4, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

dag = DAG("load_files_into_raw_data_lake",
          default_args=default_args,
          description='Load data into raw S3 data lake.',
          schedule_interval='@monthly',
          catchup=False)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_raw_datalake = CreateS3BucketOperator(
    task_id="create_raw_datalake",
    bucket_name=DefaultSettings.raw_data_lake_bucket,
    aws_connection_id="aws_credentials",
    region_name="eu-central-1",
    dag=dag
)

upload_accident_data = LoadFileToS3Operator(
    task_id='upload_accidents_data',
    bucket_name=DefaultSettings.raw_data_lake_bucket,
    aws_connection_id="aws_credentials",
    from_path="/usr/local/airflow/data/accidents",
    dag=dag
)

upload_demographic_data = LoadFileToS3Operator(
    task_id='upload_demographics_data',
    bucket_name=DefaultSettings.raw_data_lake_bucket,
    aws_connection_id="aws_credentials",
    prefix="demographics",
    from_path="/usr/local/airflow/data/demographics/us-cities-demographics.json",
    dag=dag
)

check_accidents_data_quality = DataQualityOperator(
    task_id='check_accidents_quality',
    bucket_name=DefaultSettings.raw_data_lake_bucket,
    aws_connection_id="aws_credentials",
    prefix="accidents",
    expected_count=298,
    dag=dag
)

check_demographics_data_quality = DataQualityOperator(
    task_id='check_demographics_quality',
    bucket_name=DefaultSettings.raw_data_lake_bucket,
    aws_connection_id="aws_credentials",
    prefix="demographics",
    expected_count=1,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_raw_datalake
create_raw_datalake >> upload_accident_data >> check_accidents_data_quality >> end_operator
create_raw_datalake >> upload_demographic_data >> check_demographics_data_quality >> end_operator
