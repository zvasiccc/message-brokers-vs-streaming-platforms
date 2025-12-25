from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: json.dumps(v).encode()
)

order_id = "order-101"

events = [
    {"type": "OrderCreated", "orderId": order_id},
    {"type": "OrderPaid", "orderId": order_id},
    {"type": "OrderShipped", "orderId": order_id},
    {"type": "OrderDelivered", "orderId": order_id}
]

for event in events:
    producer.send(
        "orders-events",
        key=order_id,
        value=event
    )
    print("Produced:", event)
    time.sleep(1)

producer.flush()
producer.close()
