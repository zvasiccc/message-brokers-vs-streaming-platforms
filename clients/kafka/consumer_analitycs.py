from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'user-activity',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # replay mogućnost
    group_id='analytics-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

for message in consumer:
    print(f"Consumed: key={message.key}, value={message.value}, partition={message.partition}")
