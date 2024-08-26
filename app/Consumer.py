import requests
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
        'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092',  # Адреса брокеров Kafka
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


def download():
    import key_appid as key 
    key_appid = key.key_appid
    url = f'https://api.openweathermap.org/data/2.5/weather?q=Cheboksary,ru&APPID={key_appid}&units=metric'
    r = requests.get(url=url).json()

    with open('weather_city.json', 'w') as filename:
        json.dump(r, filename)
    logging.info("Файл успешно скачан")
    return r


def process_weather_data(r):
    msc = pytz.timezone('Europe/Moscow')
    date_downloads = datetime.now(msc).strftime("%Y-%m-%d")
    time_downloads = datetime.now(msc).strftime("%H:%M:%S")
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="db_postgres",  # Название контейнера в docker-compose
                                      port="5432",
                                      database="postgres")
        logging.info("Подключение к базе PostgreSQL выполнено")
        cursor = connection.cursor()
        count_weather = insert_weather(cursor, date_downloads, time_downloads, r)
        logging.info(count_weather, "Запись успешно вставлена в таблицу 'weather'")
        count_city_name = insert_city_name(cursor)
        logging.info(count_city_name, "Запись успешно вставлена в таблицу 'city_name'")
        count_date_time_downloads = insert_date_time_downloads(cursor)
        logging.info(count_date_time_downloads, "Запись успешно вставлена в таблицу 'date_time_downloads'")
        count_sun_light = insert_sun_light(cursor)
        logging.info(count_sun_light, "Запись успешно вставлена в таблицу 'sun_light'")
        count_weather_temperature_params = insert_weather_temperature_params(cursor)
        logging.info(count_weather_temperature_params, "Запись успешно вставлена в таблицу 'weather_temperature_params'")
        count_weather_wind_clouds_params = insert_weather_wind_clouds_params(cursor)
        logging.info(count_weather_wind_clouds_params, "Запись успешно вставлена в таблицу 'weather_wind_clouds_params'")
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
