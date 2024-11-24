from kafka import KafkaConsumer
from configs import kafka_config, topic_names_dict, sensors_cfg

import json
import uuid


# todo 4. Остаточні дані:
# * Напишіть Python-скрипт, який підписується на топіки temperature_alerts та humidity_alerts, зчитує сповіщення виводить на екран повідомлення.


# ? Створення Kafka Consumer

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",  # Зчитування повідомлень з початку
    enable_auto_commit=True,  # Автоматичне підтвердження зчитаних повідомлень
    group_id="my_consumer_group_3",  # Ідентифікатор групи споживачів
)


# ? Назва топіку
topic_names = [
    topic_names_dict["temperature_alerts"],
    topic_names_dict["humidity_alerts"],
]


# ? Підписка на топік
consumer.subscribe(topic_names)
print(f"Subscribed to topic '{topic_names}'")


# ? Обробка повідомлень з топіку
try:
    for message in consumer:
        timestamp = message.value["timestamp"]
        sensor_id = message.value["id"]
        sensor = message.value["sensor"]
        sensor_value = message.value["value"]
        alert_message = message.value["alert_message"]
        print(
            # f"Received message: {message.value} with key: {message.key}, partition {message.partition}"
            f"{alert_message}, sensor_id: {sensor_id}" 
        )
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer
