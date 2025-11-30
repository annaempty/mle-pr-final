import logging as logger
import pandas as pd
import os


log = logger.getLogger("uvicorn.error")
logger.basicConfig(
    level=logger.INFO,  # включаем INFO и выше
    format='%(asctime)s [%(levelname)s] %(message)s'
)
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

PATH = {
    'personal': f's3://{S3_BUCKET_NAME}/final_pr/recommendations/recommendations.parquet',
    'default': f's3://{S3_BUCKET_NAME}/final_pr/recommendations/top_popular.parquet'
}

class Recommendations:

    def __init__(self):

        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }


    def load(self, type, **kwargs):
        """
        Загружает рекомендации
        """
        storage_options = {
            "key": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "client_kwargs": {
                "endpoint_url": "https://storage.yandexcloud.net"
            }
        }

        logger.info(f"Загружает рекомендации, type: {type}")
        self._recs[type] = pd.read_parquet(
            PATH[type], 
            storage_options=storage_options
        )
        if 'itemid' in self._recs[type].columns:
            self._recs[type].rename(columns={'itemid': 'item_id'}, inplace=True)
        if type == "personal":
            self._recs[type] = (
                self._recs[type].set_index("user_id")[["item_id"]]
                .apply(list)
            )
        logger.info("Рекомендации успешно загружены.")


    def get(self, user_id: int, k: int=100):
        """
        Возвращает список рекомендаций для пользователя
        """
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["item_id"][:k]
            recs = [int(x) for x in recs]
            self._stats["request_personal_count"] += 1
            logger.info("Загружены персональные рекомендации")
        except KeyError:
            recs = self._recs["default"]
            recs = list(recs["item_id"])[:k]
            recs = [int(x) for x in recs]
            self._stats["request_default_count"] += 1
            logger.info("Загружены дефолтные рекомендации")

        return recs


    def stats(self):

        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value} ")


if __name__ == '__main__':
    rec_store = Recommendations()

    rec_store.load(
        "personal",
        columns=["user_id", "item_id", "rank"],
    )

    rec_store.load(
        "default",
        columns=["item_id"],
    )

    logger.info(rec_store.get(user_id=2, k=5))
    logger.info(rec_store._stats)  

    