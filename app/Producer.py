import json
import requests
import logging
from confluent_kafka import Producer, KafkaException
from coord_cities import cities # Импортируем список городов

# Настройка логирования
logging.basicConfig(level=logging.INFO)

def create_producer():
    producer_conf = {
        'bootstrap.servers': 'kafka-1:9092,kafka-2:9093,kafka-3:9094',
        'acks': 'all',
        'delivery.report.only.error': False,
        'retries': 3,
    }
    try:
        with Producer(producer_conf) as producer:
            yield producer
    except KafkaException as e:
        logging.error(f"Ошибка при создании продюсера Kafka: {e}")
        raise

def on_delivery(err, msg):
    if err is not None:
        logging.error(f"Сообщение не доставлено {err}")
    else:
        logging.info(f"Сообщение доставлено в топик {msg.topic()} [{msg.partition()}] по смещению {msg.offset()}")

def download_weather_data(lat, lon):
    try:
        from key_appid import key_appid
        url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&APPID={key_appid}&units=metric'
        response = requests.get(url=url)
        response.raise_for_status()  # Проверка на успешность запроса
        data = response.json()
        return data
    except requests.RequestException as e:
        logging.error(f"Ошибка при скачивании данных о погоде: {e}")
        raise

def save_weather_data(city, data, topic):
    # Сохранение данных в файл с учетом топика
    filename = f'/home/downloads_weather/{topic}_{city}_weather.json'
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logging.info(f"Данные о погоде для {city} сохранены в файл {filename}")

def send_weather_data(producer, data, city):
    # Определяем, в какой топик отправить данные в зависимости от города
    if city in ['Vladimir', 'Voronezh', 'Cheboksary', 'Chelyabinsk', 'Ekaterinburg', 'Izhevsk']:
        topic = 'weather_topic_1'
    elif city in ['Kazan', 'Moscow', 'Nizhniy Novgorod', 'Novosibirsk', 'Penza', 'Ryazan']:
        topic = 'weather_topic_2'
    else:
        topic = 'weather_topic_3'
    try:
        message = json.dumps(data)  # Преобразование данных в JSON-формат
        producer.produce(topic, message, callback=on_delivery)  # Отправка сообщения в указанную тему
        return topic  # Возвращаем топик
    except KafkaException as e:
        logging.error(f"Ошибка при отправке сообщения в Kafka: {e}")
        raise

def main():
    try:
        producer = create_producer()
        for city, coords in cities.items():
            weather_data = download_weather_data(coords['lat'], coords['lon'])
            topic = send_weather_data(producer, weather_data, city)  # Получаем топик
            save_weather_data(city, weather_data, topic)  # Сохранение данных в файл
        producer.flush()  # Дождаться отправки всех сообщений
        logging.info("Данные о погоде отправлены в Kafka.")
    except Exception as e:
        logging.error(f"Ошибка при выполнении программы: {e}")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Действие прервано')
