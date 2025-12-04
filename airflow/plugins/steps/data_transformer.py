import pandas as pd
import numpy as np

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import sklearn
import scipy
import pickle
from tqdm.auto import tqdm
from implicit.als import AlternatingLeastSquares


SCORE = {
    'view': 1,
    'addtocart': 5,
    'transaction': 3
}
TIME_COL = 'date'
NUM = 20
MAX_SIMILAR_ITEMS = 10

class DataTransformer():
    def __init__(
            self,
            data: pd.DataFrame
    ):
        self.data=self.transform_data(data)
        self.train_df, self.test_df = self.train_test_split()
        self.user_encoder, self.item_encoder = self.create_lable_encoder()

    @staticmethod 
    def transform_data(data):
        data[TIME_COL] = pd.to_datetime(data['timestamp'], unit='ms')
        data['score'] = data.event.apply(lambda x: SCORE[x])
        return data

    def train_test_split(self):
        df = self.data.copy()
        df[TIME_COL] = pd.to_datetime(df[TIME_COL])

        # Сортируем по времени
        df = df.sort_values(TIME_COL)

        # Находим индекс разбиения
        train_size = 0.8
        split_idx = int(len(df) * train_size)

        # Выделяем train/test
        train_df = df.iloc[:split_idx]
        test_df = df.iloc[split_idx:]

        return train_df, test_df

    def create_lable_encoder(self):
        user_encoder = sklearn.preprocessing.LabelEncoder()
        user_encoder.fit(self.data["visitorid"])

        item_encoder = sklearn.preprocessing.LabelEncoder()
        item_encoder.fit(self.data["itemid"])
        return user_encoder, item_encoder


    def do_lable_encoder(self):
        self.train_df["user_idx"] = self.user_encoder.transform(self.train_df["visitorid"])
        self.test_df["user_idx"] = self.user_encoder.transform(self.test_df["visitorid"])

        self.data["item_idx"] = self.item_encoder.transform(self.data["itemid"])
        self.train_df["item_idx"] = self.item_encoder.transform(self.train_df["itemid"])
        self.test_df["item_idx"] = self.item_encoder.transform(self.test_df["itemid"])


    def make_top_popular(self):
        df_top = self.train_df\
            .groupby('itemid', as_index=False)\
            .agg({'score': 'sum'})\
            .rename(columns={'score': 'score_sum'})\
            .sort_values('score_sum', ascending=False)\
            .head(100)
        
        return df_top

    
