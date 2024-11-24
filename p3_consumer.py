from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, topic_names_dict, sensors_cfg
import json
import uuid

# todo 3. Обробка даних:
# * Напишіть Python-скрипт, який підписується на топік building_sensors, зчитує повідомлення і перевіряє отримані дані:
# * - якщо температура перевищує 40°C, генерує сповіщення і відправляє його в топік temperature_alerts;
# * - якщо вологість перевищує 80% або сягає менше 20%, генерує сповіщення і відправляє його в топік humidity_alerts.
# * Сповіщення повинні містити ідентифікатор датчика, значення показників, час та повідомлення про перевищення порогового значення.


def is_alert(sensor, sensor_value):
    if sensor_value > sensors_cfg[sensor]["alert_hi"]:
        return 1
    elif sensor_value < sensors_cfg[sensor]["alert_lo"]:
        return -1
    return 0


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

# ? Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


# ? Назва топіку
topic_name = topic_names_dict["building_sensors"]

# ? Підписка на топік
consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

# ? Обробка повідомлень з топіку
try:
    for message in consumer:
        timestamp = message.value["timestamp"]
        sensor_id = message.value["id"]
        sensor = message.value["sensor"]
        sensor_value = message.value["value"]
        print(
            # f"Received message: {message.value} with key: {message.key}, partition {message.partition}"
            f"Received message: {message.value} Value: {'Alarm' if is_alert(sensor, sensor_value) else 'Ok'}"
        )

        # * Перевірка даних
        if is_alert(sensor, sensor_value):
            # * Відправка повідомлення в топік
            try:
                alert_lo = sensors_cfg[sensor]["alert_lo"]
                alert_hi = sensors_cfg[sensor]["alert_hi"]
                units = sensors_cfg[sensor]["units"]
                alert_topic = sensors_cfg[sensor]["topic"]
                data = {
                    "timestamp": timestamp,
                    "id": sensor_id,
                    "sensor": sensor,
                    "value": sensor_value,
                    "alert_message": (
                        f"{sensor} alert: {sensor_value}{units}"
                        + (
                            f" > {alert_hi}{units}"
                            if is_alert(sensor, sensor_value) > 0
                            else f" < {alert_lo}{units}"
                        )
                    ),
                }
                producer.send(alert_topic, key=str(uuid.uuid4()), value=data)
                producer.flush()
                print(f"Message sent to topic '{alert_topic}': {data}")
            except Exception as e:
                print(f"An error occurred: {e}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer
    producer.close()  # Закриття producer
