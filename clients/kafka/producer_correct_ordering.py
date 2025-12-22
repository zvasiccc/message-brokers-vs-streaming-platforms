from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers= 'localhost:29092',
    key_serializer= lambda k: k.encode('utf-8'),
    value_serializer= lambda v: json.dumps(v).encode('utf-8')
)

order_id = "order123"

for i in range(1,10):
    msg = {
        "order_id":order_id,
        "step":i
    }
    producer.send(
        "orders",
        key=order_id,
        value=msg
    )
    print(f"Sent step {i}")
    time.sleep(1)
    
producer.flush()
producer.close()