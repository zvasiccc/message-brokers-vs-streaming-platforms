from multiprocessing import Process
import zmq

def worker(id):
    context = zmq.Context()
    pull = context.socket(zmq.PULL)
    pull.connect("ipc:///tmp/ipcTasks.ipc")
    while True:
        msg = pull.recv_string()
        print(f"Worker {id} received: {msg}")

if __name__ == "__main__":
    workers = [Process(target=worker, args=(i,)) for i in range(3)]
    for w in workers:
        w.start()
