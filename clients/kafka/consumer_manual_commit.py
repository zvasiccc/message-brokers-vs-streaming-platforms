from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:29092',
    group_id='order_group_manual',
    auto_offset_reset='earliest',

    enable_auto_commit=False, 

    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)


for message in consumer:
    try:
        print(
            f"Processing order {message.value}, partition={message.partition}, offset={message.offset}"
        )

        time.sleep(1) #simulation of business logic

        consumer.commit()

        print(f"Committed offset {message.offset + 1}")

    except Exception as e:
        print("Error:", e)
