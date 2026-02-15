import time
import random
import zmq

context = zmq.Context()

socket = context.socket(zmq.PUSH)
socket.bind("tcp://*:49152")

for i in range(20):
    message = f"task {i}"
    socket.send_string(message)
    print(f"Sent: {message}")
    # time.sleep(random.uniform(0.1, 1))
