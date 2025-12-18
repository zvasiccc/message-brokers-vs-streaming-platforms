from clients.kafka_inspecter import inspect_kafka_cluster
inspect_kafka_cluster('master_thesis_topic', ['localhost:9092', 'localhost:9093', 'localhost:9094'])