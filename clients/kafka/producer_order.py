from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

orders = [101, 102, 103,104,105,106,107,108,109,110]

while True:
    order_id = random.choice(orders)
    event = {'event': 'ORDER_CREATED', 'orderId': order_id, 'timestamp': time.time()}
    producer.send('orders', key=str(order_id), value=event)
    print(f"Sent: {event}")
    time.sleep(1)
