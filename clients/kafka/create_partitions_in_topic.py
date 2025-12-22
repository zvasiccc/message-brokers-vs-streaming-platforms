from kafka.admin import KafkaAdminClient, NewTopic
import time

KAFKA_SERVERS = ['localhost:29092']
TOPIC_NAME = "orders"

time.sleep(5)

admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_SERVERS,
    client_id='client123'
)

existing_topics = admin_client.list_topics()
if TOPIC_NAME  in existing_topics:
    admin_client.delete_topics([TOPIC_NAME])
topic = NewTopic(
    name=TOPIC_NAME,
    num_partitions=3,
    replication_factor=1
)
admin_client.create_topics([topic])
print(f"Topic '{TOPIC_NAME}' created.")

admin_client.close()
