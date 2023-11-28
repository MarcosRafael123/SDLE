import zmq
import hashlib
import json
import time

LRU_READY = "\x01"
PORT = "port:"

class Broker: 
    def __init__(self, portFrontend, portBackend, nodes=None, hash_func = hashlib.sha256): 
        self.portFrontend = portFrontend # for clients
        self.portBackend = portBackend # for servers
        self.servers = {}
        self.hash_func = hash_func
        self.ring = {}
        self.sorted_keys = []

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
                print(reply)
                if (len(reply) == 1):
                    if (PORT in reply[0].decode('utf-8')):
                        key = self.add_node(reply[0].decode('utf-8').split(':')[1].strip('"'))

                        message = str(key)

                        print(message)

                        dictionary = {}

                        dictionary["key"] = key
                        dictionary["ring"] = self.ring

                        msg[2] = json.dumps(dictionary).encode('utf-8')

                        backend.send_multipart(msg)

                else:
                    if (PORT in reply[2].decode('utf-8')):
                        key = self.add_node(reply[0].decode('utf-8').split(':')[1].strip('"'))

                        message = str(key)

                        print(message)

                        dictionary = {}

                        dictionary["key"] = key
                        dictionary["ring"] = self.ring

                        msg[2] = json.dumps(dictionary).encode('utf-8')

                        backend.send_multipart(msg)

                # Forward message to client if it's not a READY
                if reply[0] != LRU_READY:
                    frontend.send_multipart(reply)

            if socks.get(frontend) == zmq.POLLIN:
                #  Get client request, route to first available worker
                msg = frontend.recv_multipart()
                print(msg[2])
                shoppingList = json.loads(msg[2].decode('utf-8')[3:])
                print(shoppingList)

                if (shoppingList["key"] == None):
                    key = self._hash(shoppingList["url"])
                    shoppingList["key"] = key
                    msg[2] = ("sl:" + json.dumps(shoppingList)).encode('utf-8')

                request = [workers.pop(0), ''.encode()] + msg
                backend.send_multipart(request)
    
    def _hash(self, key):
        return int(self.hash_func(str(key).encode()).hexdigest(), 16)

    def add_node(self, node):
        key = self._hash(f"{node}")
        self.ring[key] = int(node)
        self.ring["timestamp"] = time.time()
        self.sorted_keys.append(key)
        self.sorted_keys.sort()
        print(self.ring)
        return key

"""     def remove_node(self, node):
        for i in range(self.replicas):
            replica_key = self._hash(f"{node}:{i}")
            del self.ring[replica_key]
            self.sorted_keys.remove(replica_key)

    def get_node(self, key):
        if not self.ring:
            return None

        h = self._hash(key)
        nodes = self.sorted_keys
        for i in range(len(nodes)):
            if h <= nodes[i]:
                return self.ring[nodes[i]]
        return self.ring[nodes[0]]
 """
        
        

def main():
    portFrontend = "5555"
    portBackend = "5556"

    broker = Broker(portFrontend, portBackend)

    broker.run()

if __name__ == "__main__":
    main()