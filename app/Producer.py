import json
import requests
import logging
from confluent_kafka import Producer, KafkaException

# Настройка логирования
logging.basicConfig(level=logging.INFO)

def create_producer():
    try:
        producer_conf = {'bootstrap.servers': 'kafka-1:9092'}
        return Producer(producer_conf)
    except KafkaException as e:
        logging.error(f"Ошибка при создании продюсера Kafka: {e}")
        raise

def on_delivery(err, msg):
    if err is not None:
        logging.error(f"Сообщение не доставлено {err}")
    else:
        logging.info(f"Сообщение доставлено в топик {msg.topic()} [{msg.partition()}] по смещению {msg.offset()}")

def download_weather_data():
    try:
        from key_appid import lat, lon, key_appid 
        url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&APPID={key_appid}&units=metric'
        response = requests.get(url=url)
        response.raise_for_status()  # Проверка на успешность запроса
        data = response.json()
        with open('weather_city.json', 'w') as filename:
            json.dump(data, filename)
        logging.info("Файл успешно скачан")
        return data
    except requests.RequestException as e:
        logging.error(f"Ошибка при скачивании данных о погоде: {e}")
        raise

def send_weather_data(producer, topic, data):
    try:
        message = json.dumps(data)  # Преобразование данных в JSON-формат
        producer.produce(topic, message, callback=on_delivery)  # Отправка сообщения в указанную тему
        producer.flush()  # Дождаться отправки всех сообщений
    except KafkaException as e:
        logging.error(f"Ошибка при отправке сообщения в Kafka: {e}")
        raise

def main():
    try:
        producer = create_producer()
        topic = 'connection_db'
        weather_data = download_weather_data()
        send_weather_data(producer, topic, weather_data)
        logging.info("Данные о погоде отправлены в Kafka.")
    except Exception as e:
        logging.error(f"Ошибка при выполнении программы: {e}")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Действие прервано')
