import paho.mqtt.client as mqtt
import time

def on_message(client, userdata, msg):
    print(f"Subscriber received: {msg.payload.decode()}")
    time.sleep(5)

client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id="client12345", 
    clean_session=False
)
client.on_message = on_message

client.connect("localhost", 1883, 60)
client.subscribe("qos/level1", qos=1)

client.loop_forever()
