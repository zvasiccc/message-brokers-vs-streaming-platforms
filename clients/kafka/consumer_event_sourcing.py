from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "orders-events",
    bootstrap_servers='localhost:29092',
    group_id="order-rebuilder",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    key_deserializer=lambda k: k.decode(),
    value_deserializer=lambda v: json.loads(v.decode())
)

order_state = {}

for msg in consumer:
    event = msg.value
    oid = event["orderId"]

    if oid not in order_state:
        order_state[oid] = []

    order_state[oid].append(event["type"])

    print(f"Order {oid} state:", order_state[oid])
