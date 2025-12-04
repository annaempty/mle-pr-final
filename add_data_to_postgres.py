import pandas as pd
from sqlalchemy import create_engine

# -------------------------------
# 1. Чтение данных из Parquet
# -------------------------------
file_path = "datasets/events_avalible.parquet"
df = pd.read_parquet(file_path)

# -------------------------------
# 2. Подключение к PostgreSQL
# -------------------------------
# Пример строки подключения:
# postgresql://username:password@host:port/dbname
db_user = ""
db_password = ""
db_host = ""
db_port = ""
db_name = ""

engine = create_engine(
    f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)

# -------------------------------
# 3. Запись в таблицу events_recomendations
# -------------------------------
# Если таблица уже есть:
# - if_exists='replace' → удалить и создать заново
# - if_exists='append' → добавить данные
df.to_sql(
    "events_recomendations",
    engine,
    if_exists="replace",
    index=False
)

print("Данные успешно записаны в таблицу events_recomendations")
