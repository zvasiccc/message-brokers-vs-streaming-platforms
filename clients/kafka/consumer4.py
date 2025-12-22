from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest', 
    group_id='group2',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

for message in consumer:
    print(f"Consumer4 consumed: key={message.key}, value={message.value}, partition={message.partition}")
