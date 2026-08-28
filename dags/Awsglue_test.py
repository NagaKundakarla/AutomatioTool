from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


def start_glue_job():
    hook = GlueJobHook(
        aws_conn_id="aws_default",
        region_name="us-east-1",
    )

    response = hook.conn.start_job_run(
        JobName="RealEstate Raw Data Handler "
    )

    job_run_id = response["JobRunId"]

    print(f"Glue Job started successfully!")
    print(f"Job Name: RealEstate Raw Data Handler")
    print(f"Job Run ID: {job_run_id}")

    return job_run_id


with DAG(
    dag_id="run_glue_job",
    start_date=datetime(2026, 8, 25),
    schedule=None,
    catchup=False,
) as dag:

    # Step 1: wait for file in S3
    wait_for_file = S3KeySensor(
        task_id="wait_for_real_estate_file",
        bucket_name="real-estate-glue-knb",
        bucket_key="TestPlugins/realestate_test_upload.csv",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=3600,
    )

    run_glue_job = PythonOperator(
        task_id="run_glue_job",
        python_callable=start_glue_job,
    )

    wait_for_file >> run_glue_job