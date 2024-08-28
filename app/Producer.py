import json
import requests
import logging
from confluent_kafka import Producer
import key_appid as key

# Настройка логирования
logging.basicConfig(level=logging.INFO)


def create_producer():
    producer_conf = {'bootstrap.servers': 'kafka-1:9092'}
    return Producer(producer_conf)


def on_delivery(err, msg):
    if err is not None:
        logging.error(f"Сообщение не доставлено {err}")
    else:
        logging.info(f"Сообщение доставлено в топик {msg.topic()} [{msg.partition()}] по смещению {msg.offset()}")


def download_weather_data():
    key_appid = key.key_appid
    url = f'https://api.openweathermap.org/data/2.5/weather?q=Cheboksary,ru&APPID={key_appid}&units=metric'
    response = requests.get(url=url)
    response.raise_for_status()  # Проверка на успешность запроса
    data = response.json()

    with open('weather_city.json', 'w') as filename:
        json.dump(data, filename)
    logging.info("Файл успешно скачан")
    return data


def send_weather_data(producer, topic, data):
    message = json.dumps(data) # Преобразование данных в JSON-формат
    producer.produce(topic, message, callback=on_delivery) # Отправка сообщения в указанную тему
    producer.flush()  # Дождаться отправки всех сообщений


def main():
    producer = create_producer()
    topic = 'connection_db'
    
    try:
        weather_data = download_weather_data()
        send_weather_data(producer, topic, weather_data)
        logging.info("Данные о погоде отпралены в Kafka.")
    except Exception as e:
        logging.error(f"Ошибка при отправке данных о погоде: {e}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Действие прервано')
