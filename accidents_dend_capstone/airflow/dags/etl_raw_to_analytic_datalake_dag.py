from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from operators import CreateS3BucketOperator, LoadFileToS3Operator
from helpers import EmrStepFactory
from helpers import DefaultSettings

default_args = {
    'owner': 'oschmi',
    'start_date': datetime(2020, 4, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

JOB_FLOW_STEPS = [
    {
        'Name': 'Install Dependencies',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['sudo', 'pip3', 'install', 'click']
        }
    },
    EmrStepFactory.get_emr_copy_step(step_name="Copy Scripts Step",
                                     source_bucket_name=DefaultSettings.source_bucket),
    EmrStepFactory.get_emr_etl_step(step_name="Extract City Demographics Step",
                                    script_name="demographics.py",
                                    raw_bucket_name=DefaultSettings.raw_data_lake_bucket,
                                    analytics_bucket_name=DefaultSettings.analytics_data_lake_bucket),
    EmrStepFactory.get_emr_etl_step(step_name="Extract Accidents and Weather Step",
                                    script_name="accidents.py",
                                    raw_bucket_name=DefaultSettings.raw_data_lake_bucket,
                                    analytics_bucket_name=DefaultSettings.analytics_data_lake_bucket),
    EmrStepFactory.get_emr_quality_step(step_name="Check City Quality Step",
                                        source_bucket_name=DefaultSettings.analytics_data_lake_bucket,
                                        prefix="cities"),
    EmrStepFactory.get_emr_quality_step(step_name="Check Accidents Quality Step",
                                        source_bucket_name=DefaultSettings.analytics_data_lake_bucket,
                                        prefix="accidents"),
    EmrStepFactory.get_emr_quality_step(step_name="Check Accidents Quality Step",
                                        source_bucket_name=DefaultSettings.analytics_data_lake_bucket,
                                        prefix="weather_conditions")
]

JOB_FLOW_OVERRIDES = {
    'Name': 'Accidents-Datalake-ETL',
}

dag = DAG('accidents_datalake_etl_dag',
          default_args=default_args,
          description='Extract transform and load data to S3 datalake.',
          schedule_interval='@monthly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_code_bucket = CreateS3BucketOperator(
    task_id="create_code_bucket",
    bucket_name=DefaultSettings.source_bucket,
    aws_connection_id="aws_credentials",
    region_name="eu-central-1",
    dag=dag
)

upload_etl_code = LoadFileToS3Operator(
    task_id="upload_etl_code",
    bucket_name=DefaultSettings.source_bucket,
    aws_connection_id="aws_credentials",
    from_path="/usr/local/airflow/spark_etl",
    dag=dag
)

create_datalake_bucket = CreateS3BucketOperator(
    task_id="create_datalake_bucket",
    bucket_name=DefaultSettings.analytics_data_lake_bucket,
    aws_connection_id="aws_credentials",
    region_name="eu-central-1",
    dag=dag
)

create_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_credentials",
    emr_conn_id="emr_default",
    region_name="eu-central-1",
    dag=dag
)

submit_jobflow_steps = EmrAddStepsOperator(
    task_id="submit_jobflow_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=JOB_FLOW_STEPS,
    dag=dag
)

check_city_processing_status = EmrStepSensor(
    task_id="check_city_demographic_processing_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='submit_jobflow_steps', key='return_value')[2] }}",
    aws_conn_id="aws_credentials",
    dag=dag
)

check_accidents_processing_status = EmrStepSensor(
    task_id="check_accidents_processing_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='submit_jobflow_steps', key='return_value')[3] }}",
    aws_conn_id="aws_credentials",
    dag=dag
)

check_city_quality_processing_status = EmrStepSensor(
    task_id="check_city_quality_processing_status",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='submit_jobflow_steps', key='return_value')[4] }}",
    aws_conn_id="aws_credentials",
    dag=dag
)

check_accidents_quality_processing_status = EmrStepSensor(
    task_id="check_accidents_quality_processing_status",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='submit_jobflow_steps', key='return_value')[5] }}",
    aws_conn_id="aws_credentials",
    dag=dag
)

check_weather_quality_processing_status = EmrStepSensor(
    task_id="check_weather_quality_processing_status",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='submit_jobflow_steps', key='return_value')[6] }}",
    aws_conn_id="aws_credentials",
    dag=dag
)

delete_cluster = EmrTerminateJobFlowOperator(
    task_id='delete_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_datalake_bucket >> create_cluster
start_operator >> create_code_bucket >> upload_etl_code >> create_cluster
create_cluster >> submit_jobflow_steps
submit_jobflow_steps >> check_city_processing_status >> check_city_quality_processing_status
submit_jobflow_steps >> check_accidents_processing_status
check_accidents_processing_status >> check_accidents_quality_processing_status
check_accidents_processing_status >> check_weather_quality_processing_status
delete_cluster << check_city_quality_processing_status
delete_cluster << check_accidents_quality_processing_status
delete_cluster << check_weather_quality_processing_status
delete_cluster >> end_operator

