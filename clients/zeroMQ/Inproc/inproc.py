import zmq
from threading import Thread
import time

context = zmq.Context()

def worker_function():
    worker_socket = context.socket(zmq.PULL)
    worker_socket.connect("inproc://tasks")  
    while True:
        msg = worker_socket.recv_string()
        print(f"Worker thread received: {msg}")

worker_thread = Thread(target=worker_function, daemon=True)
worker_thread.start()

main_socket = context.socket(zmq.PUSH)
main_socket.bind("inproc://tasks") 

for i in range(5):
    main_socket.send_string(f"task {i}")
    print(f"Main thread sent: task {i}")
    time.sleep(0.5)
