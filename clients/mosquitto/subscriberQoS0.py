import paho.mqtt.client as mqtt

def on_message(client, userdata, msg):
    print(f"Subscriber received: {msg.payload.decode()}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_message = on_message

client.connect("localhost", 1883, 60)
client.subscribe("qos/level0", qos=0)

client.loop_forever()
