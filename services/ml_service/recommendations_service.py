import os
import requests
import pandas as pd
from fastapi import FastAPI
from contextlib import asynccontextmanager

from ml_service.recomendation import Recommendations
import logging as log

logger = log.getLogger("uvicorn.error")
log.basicConfig(
    level=log.INFO,  
    format='%(asctime)s [%(levelname)s] %(message)s'
)

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
OFFLINE_RECS_PATH = f's3://{S3_BUCKET_NAME}/recsys/recommendations/recommendations.parquet'
TOP_POPULAR_PATH = f's3://{S3_BUCKET_NAME}/recsys/recommendations/top_popular.parquet'

EVENT_STORE_PORT = 8020
SIMILAR_ITEMS_PORT = 8010


rec_store = Recommendations()  


# --- lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Сервис запущен")
    # Загружаем рекомендации
    rec_store.load("personal")
    rec_store.load("default")
    try:
        yield
    finally:
        logger.info("Сервис останавливается.")


# --- Логика смешивания ---
def blend_recommendations(online_recs, offline_recs, k):
    """
    Стратегия смешивания: чередование онлайн и офлайн рекомендаций.
    Онлайн-рекомендации получают приоритет и занимают нечетные места.
    """
    blended = []
    online_idx, offline_idx = 0, 0
    
    # Удаляем дубликаты из офлайн-рекомендаций, чтобы не повторяться
    offline_recs_unique = [item for item in offline_recs if item not in online_recs]
    
    while len(blended) < k:
        # Добавляем онлайн-рекомендацию (нечетные места)
        if online_idx < len(online_recs):
            blended.append(online_recs[online_idx])
            online_idx += 1
        
        # Добавляем офлайн-рекомендацию (четные места)
        if len(blended) < k and offline_idx < len(offline_recs_unique):
            blended.append(offline_recs_unique[offline_idx])
            offline_idx += 1
            
        # Если один из списков закончился, выходим из цикла
        if online_idx >= len(online_recs) and offline_idx >= len(offline_recs_unique):
            break
            
    return blended[:k]


# --- FastAPI приложение ---
app = FastAPI(title="recommendations", lifespan=lifespan)


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100) -> dict:
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    recs_final = None
    # базовые рекомендации, всегда что-то отдает 
    recs_offline = rec_store.get(user_id=user_id, k=k)

    try:
        log.info(f"Пытаемся достать историю пользователя {user_id}")
        response = requests.get(
            f"http://events_service:{EVENT_STORE_PORT}/get", 
            params={'user_id': user_id}
        )
        log.info(f"Запрос что-то вернул {response.json()}")
        response.raise_for_status()
        user_history = response.json().get("events", [])
        log.info(user_history)
    except requests.RequestException:
        user_history = []
        log.info(f"Для пользователя {user_id} нет персональных рекомендаций")
    except Exception as e:
        log.error(f"Произошла ошибка: {e}")
    
    if len(user_history) == 0:
        log.info("Выдаем топ рекомендаций")
        return {"recs": recs_offline}

    online_recs = []
    for item_id in user_history:
        try:
            response = requests.post(
                f"http://features_service:{SIMILAR_ITEMS_PORT}/similar_items",  
                params={'item_id': item_id, 'k': k}
            )
            response.raise_for_status()
            log.info(f"{response}")
            similar = response.json().get("item_id_2", [])
            online_recs.extend(similar)
            log.info(f"{similar}")
        except requests.RequestException:
            continue

    online_recs = list(set(online_recs) - set(user_history)) 
    online_recs = list(dict.fromkeys(online_recs)) 
    
    recs_final = blend_recommendations(online_recs, recs_offline, k)
        
    return {"recs": recs_final}
