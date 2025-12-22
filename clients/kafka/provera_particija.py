from kafka.admin import KafkaAdminClient

admin = KafkaAdminClient(bootstrap_servers='localhost:29092')
topics = admin.list_topics()
print("Topics:", topics)

# Broj particija
from kafka import KafkaConsumer
consumer = KafkaConsumer('orders', bootstrap_servers='localhost:29092')
print("Partitions:", consumer.partitions_for_topic('orders'))
