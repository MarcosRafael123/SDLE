import zmq

LRU_READY = "\x01"

class Broker: 
    def __init__(self, portFrontend, portBackend): 
        self.portFrontend = portFrontend # for clients
        self.portBackend = portBackend # for servers
        self.servers = {}

    def run(self): 

        context = zmq.Context(1)

        frontend = context.socket(zmq.ROUTER) # ROUTER
        backend = context.socket(zmq.ROUTER) # ROUTER
        frontend.bind("tcp://*:" + self.portFrontend) # For clients
        backend.bind("tcp://*:" + self.portBackend)  # For workers

        poll_workers = zmq.Poller()
        poll_workers.register(backend, zmq.POLLIN)

        poll_both = zmq.Poller()
        poll_both.register(frontend, zmq.POLLIN)
        poll_both.register(backend, zmq.POLLIN)

        workers = []

        while True:
            if workers:
                socks = dict(poll_both.poll())
            else:
                socks = dict(poll_workers.poll())

            # Handle worker activity on backend
            if socks.get(backend) == zmq.POLLIN:
                # Use worker address for LRU routing
                msg = backend.recv_multipart()
                if not msg:
                    break
                address = msg[0]
                workers.append(address)

                # Everything after the second (delimiter) frame is reply
                reply = msg[2:]

                # Forward message to client if it's not a READY
                if reply[0] != LRU_READY:
                    frontend.send_multipart(reply)

            if socks.get(frontend) == zmq.POLLIN:
                #  Get client request, route to first available worker
                msg = frontend.recv_multipart()
                request = [workers.pop(0), ''.encode()] + msg
                backend.send_multipart(request)


def main():
    portFrontend = "5555"
    portBackend = "5556"

    broker = Broker(portFrontend, portBackend)

    broker.run()

if __name__ == "__main__":
    main()