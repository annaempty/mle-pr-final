import pandas as pd
import numpy as np

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from implicit.als import AlternatingLeastSquares
import sklearn
import scipy
import pickle
from tqdm.auto import tqdm


SCORE = {
    'view': 1,
    'addtocart': 5,
    'transaction': 3
}
TIME_COL = 'date'
NUM = 20
MAX_SIMILAR_ITEMS = 10


class ModelCreator():
    def __init__(
            self,
            train_df
    ):
        self.train_df = train_df.head(1000) #для теста

        self.user_encoder, self.item_encoder = self.create_lable_encoder()
        self.user_item_matrix_train = self.create_matrix_train()
        self.als_model = self.fit_model()

    def create_lable_encoder(self):
        user_encoder = sklearn.preprocessing.LabelEncoder()
        user_encoder.fit(self.train_df["visitorid"])

        item_encoder = sklearn.preprocessing.LabelEncoder()
        item_encoder.fit(self.train_df["itemid"])
        return user_encoder, item_encoder
    
    def do_lable_encoder(self):
        self.train_df["user_idx"] = self.user_encoder.transform(self.train_df["visitorid"])
        self.train_df["item_idx"] = self.item_encoder.transform(self.train_df["itemid"])

    def create_matrix_train(self):
        self.do_lable_encoder()
        events_train_agg = (
            self.train_df.groupby(["user_idx", "item_idx"], as_index=False)["score"]
            .sum()
        )
        user_item_matrix_train = scipy.sparse.csr_matrix(
            (
                events_train_agg["score"].values,                 
                (events_train_agg["user_idx"].values, events_train_agg["item_idx"].values)  
            ),
            dtype=np.int8
        )
        return user_item_matrix_train

    def fit_model(self):
        als_model = AlternatingLeastSquares(factors=50, iterations=50, regularization=0.05, random_state=0)
        als_model.fit(self.user_item_matrix_train) 
        return als_model
    
    def create_als_recomendations(self):
        # получаем список всех возможных user_id (перекодированных)
        user_ids_encoded = list(range(len(self.user_encoder.classes_)))
        # получаем рекомендации для всех пользователей
        als_recommendations = self.als_model.recommend(
            user_ids_encoded, 
            self.user_item_matrix_train, 
            filter_already_liked_items=False, 
            N=NUM
        )

        # преобразуем полученные рекомендации в табличный формат
        item_ids_enc = als_recommendations[0]
        als_scores = als_recommendations[1]
        user_ids_encoded = list(range(len(self.user_encoder.classes_)))
        
        als_recommendations_res = pd.DataFrame({
            "visitorid_enc": user_ids_encoded,
            "itemid_enc": item_ids_enc.tolist(), 
            "score": als_scores.tolist()})
        als_recommendations_res = als_recommendations_res.explode(["itemid_enc", "score"], ignore_index=True)

        # приводим типы данных
        als_recommendations_res["itemid_enc"] = als_recommendations_res["itemid_enc"].astype("int")
        als_recommendations_res["score"] = als_recommendations_res["score"].astype("float")

        # получаем изначальные идентификаторы
        als_recommendations_res["visitorid"] = self.user_encoder.inverse_transform(als_recommendations_res["visitorid_enc"])
        als_recommendations_res["itemid"] = self.item_encoder.inverse_transform(als_recommendations_res["itemid_enc"])
        als_recommendations_res = als_recommendations_res.drop(columns=["visitorid_enc", "itemid_enc"])

        return als_recommendations_res
    
    def create_similar_items(self):
        train_item_ids_enc = self.train_df['item_idx'].unique()

        # Используем ALS-модель для получения похожих объектов
        # Метод similar_items возвращает также сам объект, поэтому запрашиваем на 1 больше
        similar_items_res = self.als_model.similar_items(train_item_ids_enc, N=MAX_SIMILAR_ITEMS + 1)

        # Разделяем результат на идентификаторы и оценки сходства
        sim_item_item_ids_enc, sim_item_scores = similar_items_res

        # Преобразуем в DataFrame
        similar_items = pd.DataFrame({
            "itemid_enc": train_item_ids_enc,
            "sim_item_id_enc": sim_item_item_ids_enc.tolist(),
            "cnt_score": sim_item_scores.tolist()
        })

        # "Взрываем" списки, чтобы каждая пара оказалась в отдельной строке
        similar_items = similar_items.explode(["sim_item_id_enc", "cnt_score"], ignore_index=True)

        # Приводим типы данных
        similar_items["sim_item_id_enc"] = similar_items["sim_item_id_enc"].astype(int)
        similar_items["cnt_score"] = similar_items["cnt_score"].astype(float)

        # Декодируем обратно в оригинальные идентификаторы
        similar_items["itemid_1"] = self.item_encoder.inverse_transform(similar_items["itemid_enc"])
        similar_items["itemid_2"] = self.item_encoder.inverse_transform(similar_items["sim_item_id_enc"])

        # Убираем ненужные закодированные колонки
        similar_items = similar_items.drop(columns=["itemid_enc", "sim_item_id_enc"])

        # Исключаем пары, где объект совпадает с самим собой
        similar_items = similar_items.query("itemid_1 != itemid_2")

        return similar_items

