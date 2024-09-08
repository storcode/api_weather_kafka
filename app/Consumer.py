import json
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime
import pytz
from confluent_kafka import Consumer, KafkaError
import logging
from database import *

# Настройка логирования
logging.basicConfig(level=logging.INFO)


def main():
    consumer_conf = {
        'bootstrap.servers': 'kafka-1:9092',  # Адреса брокеров Kafka
        'group.id': 'weather_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(['connection_db'])  # Подписка на тему

    logging.info(' [*] Ожидание сообщения. Нажмите <CTRL+C> для выхода')

    try:
        while True:
            msg = consumer.poll(1.0)  # Ожидание сообщения (1 сек)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Ошибка Consumer: {msg.error()}")
                    continue

            body = msg.value().decode('utf-8')  # Декодирование сообщения
            logging.info(f"Получено сообщение: {body}")

            try:
                resp = json.loads(body)  # Преобразование JSON-строки в Python-словарь
                process_weather_data(resp)
            except json.JSONDecodeError as e:
                logging.error(f"Ошибка JSON декодирования: {e}")

    except KeyboardInterrupt:
        logging.info('Действие прервано')
    finally:
        consumer.close()


def create_connection_db():
    import key_PSQL
    connection = None
    try:
        connection = psycopg2.connect(user=key_PSQL.user,
                                      password=key_PSQL.password,
                                      host="db_postgres", # название контейнера в docker-compose
                                      port="5432",
                                      database=key_PSQL.database)
        logging.info("Подключение к базе PostgreSQL успешно")
    except OperationalError as e:
        logging.error(f"The error '{e}' occurred")
    return connection


def process_weather_data(r):
    msc = pytz.timezone('europe/moscow')
    date_downloads = datetime.now(msc).strftime("%Y-%m-%d")
    time_downloads = datetime.now(msc).strftime("%H:%M:%S")
    try:
        connection = create_connection_db()
        cursor = connection.cursor()
        count_weather = insert_weather(cursor, date_downloads, time_downloads, r)
        logging.info(f"{count_weather} Запись успешно вставлена в таблицу 'weather'")
        count_dim_clouds = insert_dim_clouds(cursor)
        logging.info(f"{count_dim_clouds} Запись успешно вставлена в таблицу 'dim_clouds'")
        count_dim_coordinates = insert_dim_coordinates(cursor)
        logging.info(f"{count_dim_coordinates} Запись успешно вставлена в таблицу 'dim_coordinates'")
        count_dim_date = insert_dim_date(cursor)
        logging.info(f"{count_dim_date} Запись успешно вставлена в таблицу 'dim_date'")
        count_dim_sun_light = insert_dim_sun_light(cursor)
        logging.info(f"{count_dim_sun_light} Запись успешно вставлена в таблицу 'dim_sun_light'")
        count_dim_time = insert_dim_time(cursor)
        logging.info(f"{count_dim_time} Запись успешно вставлена в таблицу 'dim_time'")
        count_dim_timezone = insert_dim_timezone(cursor)
        logging.info(f"{count_dim_timezone} Запись успешно вставлена в таблицу 'dim_timezone'")
        count_dim_timezone_name = insert_dim_timezone_name(cursor)
        logging.info(f"{count_dim_timezone_name} Запись успешно вставлена в таблицу 'dim_timezone_name'")
        count_dim_weather_descr = insert_dim_weather_descr(cursor)
        logging.info(f"{count_dim_weather_descr} Запись успешно вставлена в таблицу 'dim_weather_descr'")
        count_dim_wind = insert_dim_wind(cursor)
        logging.info(f"{count_dim_wind} Запись успешно вставлена в таблицу 'dim_wind'")
        count_stage_fact_weather = insert_stage_fact_weather(cursor)
        logging.info(f"{count_stage_fact_weather} Запись успешно вставлена в таблицу 'stage_fact_weather'")
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Соединение с PostgreSQL закрыто")
    except OperationalError as e:
        logging.error(f"Произошла ошибка {e}")
    except Exception as e:
        logging.error(f"Ошибка обработки данных о погоде: {e}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Действие прервано')
