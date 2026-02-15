from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:29092',
    group_id='group1',
    auto_offset_reset='earliest',
    max_poll_records=3, 
    fetch_max_wait_ms = 5000,
    fetch_min_bytes = 1000000,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

while True:
    batch = consumer.poll(5000)
    for tp, messages in batch.items():
        print(f"Received batch with {len(messages)} messages:")
        for msg in messages:
            print(f"[{time.strftime('%H:%M:%S')}] Message: {msg.value}")
        print("Batch finished.")