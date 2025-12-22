# producer_no_ordering.py
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(1, 10):
    producer.send(
        "orders",
        key=f"order-{i}", #different key
        value={"step": i}
    )

producer.flush()
producer.close()
