from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
import logging
from io import BytesIO
import boto3

import pandas as pd


TABLE_NAME = 'events_recomendations'

def load_s3_to_df(**kwargs):
    """
    Загружает CSV (или другие форматы) из S3 в pandas DataFrame
    и передаёт DataFrame в следующую таску через ti.xcom_push.
    """
    bucket_name = "s3-student-mle-20250513-c87f7369b8-freetrack"
    key = "final_pr/datasets/events_avalible.parquet" 
    conn = BaseHook.get_connection("aws_default")
    logging.info("Access Key:", conn.login)
    logging.info("Secret Key:", conn.password)

    print("Access Key:", conn.login)
    print("Secret Key:", conn.password)
    aws_access_key_id = conn.login
    aws_secret_access_key = conn.password
    region_name = "us-east-1"

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    # Получаем объект из S3
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)

    # Читаем содержимое CSV в DataFrame
    df = pd.read_parquet(BytesIO(obj['Body'].read()))
    logging.info(df.head())
    logging.info(df.columns)



def create_table(**kwargs) -> None:
    """
    Создает таблицу в PostgreSQL и записывает туда данные из DataFrame.
    """
    from sqlalchemy import (
        MetaData, Table, Column, String, Integer, DateTime, Float,
        UniqueConstraint, inspect
    )
    hook = PostgresHook(postgres_conn_id='destination_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData()

    # Определяем таблицу
    events_table = Table(
        TABLE_NAME,
        metadata,
        Column('timestamp', DateTime, nullable=False),
        Column('visitorid', String, nullable=False),
        Column('event', String, nullable=False),
        Column('itemid', String, nullable=True),
        Column('transactionid', String, nullable=True),
    )

    if not inspect(engine).has_table(events_table.name):
        metadata.create_all(engine)


def load(**kwargs):
    # Записываем данные в таблицу
    hook = PostgresHook('destination_db')
    data = kwargs['ti'].xcom_pull(key="df_from_s3", task_ids="load_s3_to_df_task")

    hook.insert_rows(
        table=TABLE_NAME,
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['customer_id'],
        rows=data.values.tolist()
    ) 
    print(f"Данные успешно записаны в таблицу '{TABLE_NAME}'")

