import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.add_data_to_postgres import (
    load_s3_to_df,
    create_table,
    load
)


with DAG(
    dag_id='add_data_to_postgres',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
) as dag:
    create_table_task = PythonOperator(task_id='create_table_task', python_callable=create_table)
    load_s3_to_df_task = PythonOperator(task_id='load_s3_to_df_task', python_callable=load_s3_to_df)
    load_task = PythonOperator(task_id='load_task', python_callable=load)

    create_table_task >> load_s3_to_df_task >> load_task
    