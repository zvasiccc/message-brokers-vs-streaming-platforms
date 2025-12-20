from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

users = ['alice', 'bob', 'carol']

while True:
    user = random.choice(users)
    event = {'event': 'USER_LOGIN', 'user': user, 'timestamp': time.time()}
    producer.send('user-activity', key=user, value=event)
    print(f"Sent: {event}")
    time.sleep(1)
