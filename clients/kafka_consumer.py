from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time

KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'master_rad_topic'
MAX_RETRIES = 10 
RETRY_DELAY = 2 

def create_consumer_earliest():
    consumer = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Connecting to kafka {attempt}/{MAX_RETRIES}...")
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_SERVER],
                group_id='master_rad_group',
                auto_offset_reset='earliest', 
                enable_auto_commit=True,
                value_deserializer=lambda m: m.decode('utf-8'),
                session_timeout_ms=10000 
            )
            print("Connected succesfully")
            return consumer
        
        except NoBrokersAvailable:
            if attempt == MAX_RETRIES:
                raise 
            time.sleep(RETRY_DELAY)
        except Exception as e:

            print(f"Exception: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY)
    return None

def create_consumer_latest():
    consumer = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Connecting to kafka {attempt}/{MAX_RETRIES}...")
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_SERVER],
                group_id='master_rad_group',
                auto_offset_reset='earliest', 
                enable_auto_commit=True,
                value_deserializer=lambda m: m.decode('utf-8'),
                session_timeout_ms=10000 
            )
            print("Connected succesfully")
            return consumer
        
        except NoBrokersAvailable:
            if attempt == MAX_RETRIES:
                raise 
            time.sleep(RETRY_DELAY)
        except Exception as e:

            print(f"Exception: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY)
    return None

def run_consumer():

    consumer = None
    try:
        consumer = create_consumer_latest()
        if consumer is None:
            return

        print(f"Listening topic '{TOPIC_NAME}'...")
        
        for message in consumer:
            receive_time = time.time() 
            
            try:
                data = json.loads(message.value)
                send_time = data.get('timestamp')
            
                latency_ms = (receive_time - send_time) * 1000
                print(f"Received: ID {data['id']} | Latention is: {latency_ms:.2f}ms | Data: {data['payload']}")

                    
            except json.JSONDecodeError:
                print(f"Message error: {message.value}")
                
    except Exception as e:
        print(f"Exception: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Consumed closed.")


if __name__ == '__main__':
    run_consumer()