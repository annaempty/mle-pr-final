import pandas as pd
import numpy as np
from io import BytesIO
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from implicit.als import AlternatingLeastSquares

from steps.quality_assessment import QualityAssessment
from steps.data_transformer import DataTransformer
from steps.model_creator import ModelCreator


TABLE_NAME = 'events_recomendations'
SCORE = {
    'view': 1,
    'addtocart': 5,
    'transaction': 3
}
TIME_COL = 'date'
NUM = 20
MAX_SIMILAR_ITEMS = 10


def extract(**kwargs):
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    sql = f"""
        select *
        from events_recomendations
    """
    data = pd.read_sql(sql, conn)
    conn.close()

    logging.info(data.head(5))
    kwargs['ti'].xcom_push('extracted_data', data)


def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key="extracted_data", task_ids="extract_task")

    data_transformer = DataTransformer(data=data)
    train_df = data_transformer.train_df
    test_df = data_transformer.test_df

    top_popular = data_transformer.make_top_popular()

    kwargs['ti'].xcom_push('train_df', train_df)
    kwargs['ti'].xcom_push('test_df', test_df)
    kwargs['ti'].xcom_push('top_popular', top_popular)


def create_model(**kwargs):
    ti = kwargs['ti']
    train_df = ti.xcom_pull(key="train_df", task_ids="transform_data_task")
    test_df = ti.xcom_pull(key="test_df", task_ids="transform_data_task")
    top_popular = ti.xcom_pull(key="top_popular", task_ids="transform_data_task")

    model_creator = ModelCreator(
        train_df=train_df,
    )
    als_recommendations = model_creator.create_als_recomendations()
    similar_items_res = model_creator.create_similar_items()
    kwargs['ti'].xcom_push('als_recommendations', als_recommendations)
    kwargs['ti'].xcom_push('similar_items_res', similar_items_res)


def calqulate_quality(**kwargs):
    ti = kwargs['ti']
    train_df = ti.xcom_pull(key="train_df", task_ids="transform_data_task")
    test_df = ti.xcom_pull(key="test_df", task_ids="transform_data_task")
    top_popular = ti.xcom_pull(key="top_popular", task_ids="transform_data_task")
    als_recommendations = ti.xcom_pull(key="als_recommendations", task_ids="create_model_task")

    logging.info(test_df)
    logging.info(train_df)
    logging.info(top_popular)
    
    k = 10
    quality_als = QualityAssessment(
        top_rec=False,
        events_test=test_df,
        events_train=train_df,
        top_popular=top_popular,
        als_recommendations=als_recommendations,
        k=k
    )
    quality_top = QualityAssessment(
        top_rec=True,
        events_test=test_df,
        events_train=train_df,
        top_popular=top_popular,
        als_recommendations=als_recommendations,
        k=k
    )
    metrics = {}
    metrics['Personal ALS'] = {
        f'precision@{k}': quality_als.precision_at_k(),
        f'recall@{k}': quality_als.recall_at_k(),
        f'coverage@{k}': quality_als.coverage_at_k(),
        f'novelty@{k}': quality_als.novelty_at_k()
    }
    metrics['TOP'] = {
        f'precision@{k}': quality_top.precision_at_k(),
        f'recall@{k}': quality_top.recall_at_k(),
        f'coverage@{k}': quality_top.coverage_at_k(),
        f'novelty@{k}': quality_top.novelty_at_k()
    }
    kwargs['ti'].xcom_push('metrics', metrics)


def load_to_s3(**kwargs):
    bucket_name = "s3-student-mle-20250513-c87f7369b8-freetrack"
    s3_folder = "final_pr/new_data/" 
    ti = kwargs["ti"]

    top_popular = ti.xcom_pull(key="top_popular", task_ids="transform_data_task")
    als_recommendations = ti.xcom_pull(key="als_recommendations", task_ids="create_model_task")
    similar_items_res = ti.xcom_pull(key="similar_items_res", task_ids="create_model_task")

    files = {
        "top_popular.parquet": top_popular,
        "als_recommendations.parquet": als_recommendations,
        "similar_items_res.parquet": similar_items_res
    }

    hook = S3Hook(aws_conn_id="aws_default")

    for file_name, df in files.items():
        if df is None:
            raise ValueError(f"No DataFrame found in XCom for {file_name}")

        # Сохраняем DataFrame в буфер
        buffer = BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        # Загружаем в S3
        s3_key = f"{s3_folder}{file_name}"
        hook.load_file_obj(
            file_obj=buffer,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"Uploaded DataFrame to s3://{bucket_name}/{s3_key}")
