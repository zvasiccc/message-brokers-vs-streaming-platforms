import zmq
import time
import random

context = zmq.Context()

socket = context.socket(zmq.PULL)
socket.connect("tcp://localhost:49152")

while True:
    message = socket.recv_string()
    print(f"Consumer3  is processing: {message}")
    time.sleep(random.uniform(1, 2))
