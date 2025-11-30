import logging as logger
import requests
import time
import pandas as pd
import os

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Глобальные переменные
EVENT_STORE_PORT = 8020
REC_SERVICE_PORT = 8000

USER_WITH_RECS = 6
USER_WITHOUT_RECS = 1
ADD_ITEM = [355908, 10405, 147168, 403063, 116711]

log = logger.getLogger("uvicorn.error")
logger.basicConfig(
    level=logger.INFO,  
    format='%(asctime)s [%(levelname)s] %(message)s'
)

def test_endpoint(description, user_id):
    logger.info(f"{description}")
    try:
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        params = {"user_id": user_id, 'k': 10}
        resp = requests.post(f"http://127.0.0.1:{REC_SERVICE_PORT}/recommendations", headers=headers, params=params)
        if resp.status_code == 200:
            recs = resp.json()
        else:
            recs = []
            logger.info(f"status code: {resp.status_code}")
        logger.info(recs)     
    except requests.RequestException as e:
        logger.info(f"Ошибка запроса: {e}")
    logger.info("-" * 25 + "\n")


def add_event(user_id, items_id):
    logger.info(f"Добавляем событие: пользователь {user_id}, трек {items_id}")
    try:
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        for item in items_id:
            params = {"user_id": user_id, "item_id": item}
            resp = requests.post(f"http://127.0.0.1:{EVENT_STORE_PORT}/put", headers=headers, params=params)
            if resp.status_code == 200:
                result = resp.json()
            else:
                result = None
                logger.info(f"status code: {resp.status_code}")
            time.sleep(0.5)
            
            logger.info(result) 
    except requests.RequestException as e:
        logger.info(f"Ошибка при добавлении события: {e}")


if __name__ == '__main__':
    logger.info("Тестируем рекомендации\n")
    
    test_endpoint("1) Пользователь без персональных рекомендаций (ожидаем топ популярных)", USER_WITHOUT_RECS)
    
    test_endpoint(f"2)Пользователь {USER_WITH_RECS} с офлайн, без онлайн рекомендаций", USER_WITH_RECS)

    logger.info(f"3) Пользователь {USER_WITH_RECS} с офлайн и онлайн  рекомендациями")
    add_event(USER_WITH_RECS, ADD_ITEM)

    logger.info("\n")
    logger.info("4)Смешанные рекомендации")
    test_endpoint(f"Результат для пользователя {USER_WITH_RECS} после добавления событий", USER_WITH_RECS)

    logger.info("Конец :) ")
    