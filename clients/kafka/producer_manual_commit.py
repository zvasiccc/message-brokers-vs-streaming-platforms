from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(1, 21):
    msg = {"step": i}
    producer.send("orders", key="order123", value=msg)
    print(f"Sent step {i}")
    time.sleep(5) 
