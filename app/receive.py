import requests
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime
import pytz
from database import *


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='connection_db', durable=True)  # нужно убедиться, что очередь переживет перезапуск
    # RabbitMQ, для этого нам нужно объявить его устойчивым

    def do_work(ch, method, properties, body):
        resp = download()
        process_weather_data(resp)

    channel.basic_consume(queue='connection_db', on_message_callback=do_work, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def download():
    import key_appid as key 
    key_appid = key.key_appid
    url = f'https://api.openweathermap.org/data/2.5/weather?q=Cheboksary,ru&APPID={key_appid}&units=metric'
    r = requests.get(url=url).json()

    with open('weather_city.json', 'w') as filename:
        json.dump(r, filename)
    print("Файл успешно скачан")
    return r


def process_weather_data(r):
    msc = pytz.timezone('europe/moscow')
    date_downloads = datetime.now(msc).strftime("%Y-%m-%d")
    time_downloads = datetime.now(msc).strftime("%H:%M:%S")
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="db_app",  # название контейнера в docker-compose
                                      port="5432",
                                      database="postgres")
        print("Подключение к базе PostgreSQL выполнено")
        cursor = connection.cursor()
        count_weather = insert_weather(cursor, date_downloads, time_downloads, r)
        print(count_weather, "Запись успешно вставлена в таблицу 'weather'")
        count_city_name = insert_city_name(cursor)
        print(count_city_name, "Запись успешно вставлена в таблицу 'city_name'")
        count_date_time_downloads = insert_date_time_downloads(cursor)
        print(count_date_time_downloads, "Запись успешно вставлена в таблицу 'date_time_downloads'")
        count_sun_light = insert_sun_light(cursor)
        print(count_sun_light, "Запись успешно вставлена в таблицу 'sun_light'")
        count_weather_temperature_params = insert_weather_temperature_params(cursor)
        print(count_weather_temperature_params, "Запись успешно вставлена в таблицу 'weather_temperature_params'")
        count_weather_wind_clouds_params = insert_weather_wind_clouds_params(cursor)
        print(count_weather_wind_clouds_params, "Запись успешно вставлена в таблицу 'weather_wind_clouds_params'")
        connection.commit()
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
    except OperationalError as e:
        print(f"Произошла ошибка {e}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
