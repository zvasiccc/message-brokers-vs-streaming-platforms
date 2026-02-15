from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(1, 11):
    message_key = f"order-{i}"
    message_value = {"step": i}
    producer.send(
        "orders",
        key=message_key, 
        value=message_value
    )
    print(f"Producer sent: key:{message_key}, value: {message_value}")

producer.flush()
producer.close()
