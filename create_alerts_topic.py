from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
)

MY_NAME = "roman.ianivskyi"
new_topic = NewTopic(name=f"{MY_NAME}_alerts", num_partitions=2, replication_factor=1)

try:
    admin_client.create_topics(new_topics=[new_topic])
    print(f"Topic '{new_topic.name}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    admin_client.close()