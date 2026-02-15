import zmq
import time

context = zmq.Context()

pub_socket = context.socket(zmq.PUB)
pub_socket.bind("tcp://*:49152")

time.sleep(1)

for i in range(1, 11):
    message = f"Message {i}"
    pub_socket.send_string(message)
    print(f"Sent: {message}")
    time.sleep(0.5) 
