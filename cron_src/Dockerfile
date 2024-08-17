FROM python:3.11

RUN apt update && apt -y install cron
RUN pip install --upgrade pip
RUN pip install requests
RUN pip install psycopg2-binary
RUN pip install DateTime
RUN pip install pytz

# Скопировать cron-docker файл в каталог cron.d 
COPY ./cron_docker /etc/cron.d/crontab
 
# Предоставить права на выполнение задания cron
RUN chmod 0644 /etc/cron.d/crontab

# Применить задание cron
RUN /usr/bin/crontab /etc/cron.d/crontab
 
# Создать файл журнала, чтобы иметь возможность запускать tail
RUN touch /var/log/cron_docker.log
 
# Запустить команду при запуске контейнера
CMD ["cron", "-f"]