from kafka import KafkaProducer
import json
import time
from kafka.errors import NoBrokersAvailable

KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'master_rad_topic'
MESSAGE_COUNT = 10
MAX_RETRIES = 10
RETRY_DELAY = 2

def create_producer():
    
    producer = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Connecting to kafka {attempt}/{MAX_RETRIES}...")
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
            )
            print("Connected succesfully")
            return producer
        
        except NoBrokersAvailable:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY)
        except Exception as e:
            print(f"Exception: {e}")
            time.sleep(RETRY_DELAY)
    return None


def run_producer():

    producer = None
    try:
        producer = create_producer()
        if producer is None:
            return

        print(f"Sending {MESSAGE_COUNT} messages to topic '{TOPIC_NAME}'...")
        
        for i in range(MESSAGE_COUNT):
            message = {
                'id': i + 1,
                'timestamp': time.time(),
                'payload': f'Message number {i + 1}'
            }
            
            future = producer.send(TOPIC_NAME, message)
            
            try:

                record_metadata = future.get(timeout=10)
                print(f"Sent: ID {message['id']} -> Offset: {record_metadata.offset}")
            except Exception as e:
                print(f"Exception: {e}")

            time.sleep(0.1)
            
        
    except Exception as e:
        print(f"Exception: {e}")

    finally:
        if producer:
            producer.close()
            print("Producer closed")

if __name__ == '__main__':
    run_producer()