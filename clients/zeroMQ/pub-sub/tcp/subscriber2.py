import zmq

context = zmq.Context()

sub_socket = context.socket(zmq.SUB)
sub_socket.connect("tcp://localhost:49152")

sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

while True:
    msg = sub_socket.recv_string()
    print(f"Subscriber2 is processing: {msg}")
