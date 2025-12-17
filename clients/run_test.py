# clients/run_test.py

from kafka_client import KafkaBenchmark, kafka_subscribe
# from rabbitmq_client import RabbitMQBenchmark, rabbitmq_subscribe  # Dodati kasnije
import time
import json
import threading
import pandas as pd

MESSAGE_COUNT = 1000

def receiver_callback(system, data):
    """Funkcija koja se poziva kada se primi poruka."""
    received_at = time.time()
    latency_ms = (received_at - data['sent_at']) * 1000
    
    print(f"[{system.upper()} - {data['id']}] Primljeno. Latencija: {latency_ms:.2f}ms")
    # OVDE BI SE REZULTATI UPISIVALI U CSV FAJL

def run_kafka_test():
    """Pokreće test za Kafku."""
    
    # 1. Pokreni Potrošača u pozadini (threading)
    consumer_thread = threading.Thread(target=kafka_subscribe, args=(receiver_callback,))
    consumer_thread.daemon = True # Omogućava da se nit ugasi kada se main program završi
    consumer_thread.start()
    
    # 2. Sačekaj da se Potrošač poveže
    time.sleep(5) 
    
    # 3. Pokreni Producenta
    producer = KafkaBenchmark()
    print("\n--- POKRETANJE KAFKA TESTA ---")
    start_time = time.time()

    for i in range(1, MESSAGE_COUNT + 1):
        msg = {'id': i, 'payload': f'Poruka {i}'}
        producer.publish_message(msg)
        # Opciono: time.sleep(0.001) za kontrolu propusnosti

    producer.close()
    
    end_time = time.time()
    throughput = MESSAGE_COUNT / (end_time - start_time)
    print(f"--- Slanje završeno ---")
    print(f"Propusnost (Throughput): {throughput:.2f} poruka/sekundi")
    
    # 4. Sačekaj da potrošač primi sve poruke
    time.sleep(5) 

if __name__ == '__main__':
    run_kafka_test()
    # run_rabbitmq_test() # Dodati kasnije