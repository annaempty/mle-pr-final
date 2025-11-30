import logging as logger
import os
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI


log = logger.getLogger("uvicorn.error")
logger.basicConfig(
    level=logger.INFO,  # включаем INFO и выше
    format='%(asctime)s [%(levelname)s] %(message)s'
)

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
PATH = f's3://{S3_BUCKET_NAME}/final_pr/recommendations/similar.parquet'  


class SimilarItems:

    def __init__(self):
        self._similar_items = None

    def load(self, path=PATH, **kwargs):
        """
        Загружаем данные из файла
        """
        storage_options = {
            "key": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "client_kwargs": {
                "endpoint_url": "https://storage.yandexcloud.net"
            }
        }

        logger.info(f"Загрузка файла из {path}")
        self._similar_items = pd.read_parquet(
            path,
            storage_options=storage_options
        )

        logger.info(str(self._similar_items.columns))
        logger.info(str(self._similar_items))
        self._similar_items.columns = ['cnt_score', 'item_id_1', 'item_id_2']
        self._similar_items = (
            self._similar_items
            .sort_values(['item_id_1', 'cnt_score'])
            .set_index('item_id_1')
        )

        logger.info(self._similar_items.head())
        logger.info("Данные успешно загружены")

    def get(self, item_id: int, k: int = 5):
        """
        Возвращает список похожих объектов
        """
        try:
            log.info(item_id)
            i2i = self._similar_items.loc[item_id].head(k)
            log.info(i2i)
            i2i = i2i[["item_id_2", "cnt_score"]].to_dict(orient="list")
            log.info(i2i)
        except KeyError:
            logger.error(f"Нет похожих объектов для item_id={item_id}")
            i2i = {"item_id_2": [], "score": []}
        return i2i


sim_items_store = SimilarItems()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        sim_items_store.load()
        logger.info("Сервис готов к работе!")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
    yield
    logger.info("Сервис останавливается...")


app = FastAPI(title="features", lifespan=lifespan)


@app.post("/similar_items")
async def recommendations(item_id: int, k: int = 5):
    """
    Возвращает список похожих объектов длиной k для item_id
    """
    return sim_items_store.get(item_id, k)
