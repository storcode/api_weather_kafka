# import pika


# connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))  # в docker-compose так называется сервис
# channel = connection.channel()
# channel.queue_declare(queue='connection_db', durable=True)  # нужно убедиться, что очередь переживет перезапуск RabbitMQ, для этого нам нужно объявить его устойчивым
# channel.basic_publish(
#     exchange='',
#     routing_key='connection_db',
#     body='conn_db_dwn_json',
#     properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)   # очередь задач не потеряется при перезапуске кролика
#     )
# print("Sent message")
# connection.close()
