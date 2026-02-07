import zmq
import time

context = zmq.Context()
push_socket = context.socket(zmq.PUSH)
push_socket.bind("ipc:///tmp/ipcTasks.ipc")  

for i in range(5):
    msg = f"task {i}"
    push_socket.send_string(msg)
    print(f"Producer sent: {msg}")
    time.sleep(0.5) 
