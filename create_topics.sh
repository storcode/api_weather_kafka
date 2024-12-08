#!/bin/bash

set -euo pipefail

# set -e - завершает скрипт, если команда возвращает ненулевой код выхода.
# set -u - завершает скрипт, если переменная используется без присвоения значения.
# set -o pipefail - завершает скрипт, если команда в конвейере возвращает ненулевой код выхода.

BOOTSTRAP_SERVER="kafka-1:9091,kafka-2:9092,kafka-3:9093"  # Имя сервиса Kafka в Docker Compose

# Проверка доступности Kafka
while ! kafka-topics --list --bootstrap-server "$BOOTSTRAP_SERVER" >/dev/null 2>&1; do
  echo "Ожидание запуска Kafka..."
  sleep 10
done

create_topic() {
  TOPIC_NAME="weather_topic_$1"
  echo "Создание топика: $TOPIC_NAME"
  kafka-topics --create --topic "$TOPIC_NAME" --bootstrap-server "$BOOTSTRAP_SERVER" --partitions 1 --replication-factor 1
  if [ $? -ne 0 ]; then
    echo "Ошибка при создании топика: $TOPIC_NAME"
    exit 1
  fi
    echo "Топик $TOPIC_NAME создан."
}

for i in {1..3}; do
  create_topic "$i"
done

echo "Все топики созданы."