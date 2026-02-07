import paho.mqtt.client as mqtt
import time

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

client.connect("localhost", 1883, 60)

client.loop_start()

for i in range(10):
    payload_message = f"Message with QoS 1, number {i}"
    
    client.publish(
        topic="qos/level1",
        payload=payload_message,
        qos=1
    )
    
    print(f"Producer sent: {payload_message}")
    
    time.sleep(1)


time.sleep(1)

client.loop_stop()
client.disconnect()