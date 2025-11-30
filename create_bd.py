import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, Table

def save_events_to_postgres(events_df: pd.DataFrame, db_url: str, table_name: str = "events"):
    """
    Создает таблицу в PostgreSQL и записывает туда данные из DataFrame.

    :param events_df: pandas DataFrame с колонками:
                      ['timestamp', 'visitorid', 'event', 'itemid', 'transactionid']
    :param db_url: URL подключения к PostgreSQL, например:
                   "postgresql://username:password@localhost:5432/mydatabase"
    :param table_name: имя таблицы в базе данных
    """
    # Создаем подключение к БД
    engine = create_engine(db_url)
    metadata = MetaData()

    # Определяем таблицу
    events_table = Table(
        table_name,
        metadata,
        Column('timestamp', DateTime, nullable=False),
        Column('visitorid', String, nullable=False),
        Column('event', String, nullable=False),
        Column('itemid', String, nullable=True),
        Column('transactionid', String, nullable=True),
    )

    # Создаем таблицу в БД (если не существует)
    metadata.create_all(engine)

    # Записываем данные в таблицу
    events_df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Данные успешно записаны в таблицу '{table_name}'")


if __name__ == '__main__':
    df = pd.read_csv('datasets/events_avalible.parquet')

    save_events_to_postgres(
        df,
        
    )