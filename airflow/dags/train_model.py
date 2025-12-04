import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.train_model import (
    extract,
    transform_data,
    create_model,
    calqulate_quality,
    load_to_s3
)


with DAG(
    dag_id='train_model',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
) as dag:
    extract_task = PythonOperator(task_id='extract_task', python_callable=extract)
    transform_data_task = PythonOperator(task_id='transform_data_task', python_callable=transform_data)
    create_model_task = PythonOperator(task_id='create_model_task', python_callable=create_model)
    calqulate_quality_task = PythonOperator(task_id='calqulate_quality_task', python_callable=calqulate_quality)
    load_to_s3_task = PythonOperator(task_id='load_to_s3_task', python_callable=load_to_s3)

    extract_task >> transform_data_task >> create_model_task >> calqulate_quality_task >> load_to_s3_task
    