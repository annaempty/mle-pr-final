from fastapi import FastAPI
import logging as log
from ml_service.events import EventStore
from prometheus_fastapi_instrumentator import Instrumentator

logger = log.getLogger("uvicorn.error")
log.basicConfig(
    level=log.INFO,  
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# создаём глобальный стор

events_store = EventStore()

# создаём приложение FastAPI

app = FastAPI(title="events")

instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)

@app.post("/put")
async def put(user_id: int, item_id: int):
    """
    Сохраняет событие для user_id, item_id
    """
    log.info(f"Сохранение события item_id = {item_id}, для user_id = {user_id}")
    log.info(f"events_store ДО сохранения {events_store.events}")
    events_store.put(user_id, item_id)
    log.info(f"events_store ПОСЛЕ сохранения {events_store.events}")
    return {"result": "ok"}

@app.get("/get")
async def get(user_id: int, k: int = 10):
    """
    Возвращает список последних k событий для пользователя user_id
    """
    events = events_store.get(user_id, k)
    log.info(events)
    log.info(type(events))
    return {"events": events}

