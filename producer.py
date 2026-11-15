from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

# Назва топіку
MY_NAME = "roman.ianivskyi"
topic_name = f"{MY_NAME}_building_sensors"
sensor_id = random.randint(1, 1000)

print(f"Sensor {sensor_id} started sending data to '{topic_name}'...")

for i in range(100):
    try:
        data = {
            "sensor_id": sensor_id,
            "timestamp": time.time(),
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85),
        }
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()
        print(f"[{i + 1} out of 100] Sent: {data}")
        time.sleep(random.randint(1, 5))
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()