import zmq
from threading import Thread
import time

context = zmq.Context()

def worker_function(name):
    socket = context.socket(zmq.PULL)
    socket.connect("inproc://tasks")
    while True:
        msg = socket.recv_string()
        print(f"{name} received: {msg}")

for i in range(3):
    t = Thread(target=worker_function, args=(f"Worker {i}",), daemon=True)
    t.start()

push = context.socket(zmq.PUSH)
push.bind("inproc://tasks")

for i in range(9):
    push.send_string(f"task {i}")
    print(f"Main thread sent: task {i}")
    time.sleep(0.2)
