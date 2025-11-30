import logging as log

logger = log.getLogger("uvicorn.error")
log.basicConfig(
    level=log.INFO,  
    format='%(asctime)s [%(levelname)s] %(message)s'
)

class EventStore:

    def __init__(self, max_events_per_user=10):
        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        """
        Сохраняет событие
        """
        user_events = self.events.get(user_id, [])  
        self.events[user_id] = [item_id] + user_events[: self.max_events_per_user]
        log.info(f"in put: {self.events}")

    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        if user_id in self.events.keys():
            log.info(f"У пользователя {user_id} была найдена история ивентов")
            return self.events.get(user_id)[:k]  
        log.info(f"У пользователя {user_id} НЕТ ивентов")
        return []
    