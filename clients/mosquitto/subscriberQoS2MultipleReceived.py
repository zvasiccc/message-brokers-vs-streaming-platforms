import paho.mqtt.client as mqtt
import time

def on_message(client, userdata, msg):
    print(f"Subscriber received: {msg.payload.decode()}")
    time.sleep(5)

client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id="client1234567", 
    clean_session=False
)

client.max_inflight_messages_set(1)

client.on_message = on_message

client.connect("localhost", 1883, 60)
client.subscribe("qos/level2", qos=2)

client.loop_forever()
