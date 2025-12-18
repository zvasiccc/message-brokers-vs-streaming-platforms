from kafka import KafkaConsumer

def inspect_kafka_cluster(topic_name, bootstrap_servers):
    """
    Connects to the cluster and prints metadata about brokers and partitions.
    """
    try:
        # Koristimo Consumer jer on lako povlači metapodatke o celom klasteru
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers
        )
        
        # Uzimamo metapodatke za traženi topik
        partitions = consumer.partitions_for_topic(topic_name)
        
        if partitions is None:
            print(f"Topic '{topic_name}' does not exist yet. Send a message to create it.")
            return

        print(f"\n--- Metadata for Topic: {topic_name} ---")
        for partition_id in partitions:
            # Tražimo informaciju o svakoj particiji pojedinačno
            print(f"Partition: [{partition_id}]")
            
            # Napomena: biblioteka automatski mapira lidere
            # Možeš videti listu svih dostupnih brokera
            print(f"Available Brokers: {bootstrap_servers}")
            
        print("-------------------------------------------\n")
        consumer.close()
        
    except Exception as e:
        print(f"Could not fetch metadata: {e}")

