# clients/kafka_client.py

from kafka import KafkaProducer, KafkaConsumer
import json
import time

KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'master_rad_topic'

class KafkaBenchmark:
    
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def publish_message(self, message):
        # Slanje poruke i beleženje vremena slanja
        message['sent_at'] = time.time() 
        future = self.producer.send(TOPIC_NAME, message)
        
        # Opciono: Flush nakon svake poruke radi tačnijeg merenja
        self.producer.flush()
        return future 

    def close(self):
        self.producer.close()
        
# Funkcija za slušanje (Potrošač)
def kafka_subscribe(callback):
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_SERVER],
        group_id='master_rad_group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode('utf-8')
    )
    for message in consumer:
        try:
            data = json.loads(message.value)
            callback("kafka", data)
        except Exception as e:
            print(f"Greška u obradi Kafka poruke: {e}")