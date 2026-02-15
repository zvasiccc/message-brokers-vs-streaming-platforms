import zmq

context = zmq.Context()
pull_socket = context.socket(zmq.PULL)
pull_socket.connect("ipc:///tmp/ipcTasks.ipc") 

while True:
    msg = pull_socket.recv_string()
    print(f"Consumer received: {msg}")


