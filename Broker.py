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
        self.hash_func = hash_func
        self.ring = {}
        self.sorted_keys = []

    def redirect_shopping_list(self, shoppinglist):
        sl_key = shoppinglist["key"]

        my_servers = self.ring.copy()
        del my_servers["timestamp"]
        print("MY_SERVERS: ", my_servers)

        first_key = int('9' * 100)
        server_to_send = None
        for key, value in my_servers.items():
            print("KEY: ", key)
            print("SL_KEY: ", sl_key)
            if int(key) < first_key:
                first_key = int(key)
                first_key_value = int(value)
            
            if int(key) < sl_key:
                print("ENTERED HERE")
                continue
            else:
                server_to_send = int(value)
                return server_to_send                

        print("FIRST_KEY: ", first_key)
        print("FIRST_KEY_VALUE: ", first_key_value)
        server_to_send = int(first_key_value)
        return server_to_send

    def run(self): 

        context = zmq.Context(1)

        frontend = context.socket(zmq.ROUTER)
        backend = context.socket(zmq.ROUTER) 
        frontend.bind("tcp://*:" + self.portFrontend)
        backend.bind("tcp://*:" + self.portBackend) 

        poll_both = zmq.Poller()
        poll_both.register(frontend, zmq.POLLIN)
        poll_both.register(backend, zmq.POLLIN)

        while True:
            sockets = dict(poll_both.poll())

            if backend in sockets and sockets[backend] == zmq.POLLIN:
                msg = backend.recv_multipart()

                print(msg)
                reply = msg[1]
                print(reply)
                
                if (PORT in reply.decode('utf-8')):
                    key = self.add_node(reply.decode('utf-8').split(':')[1].strip('"'))

                    message = str(key)

                    print(message)

                    dictionary = {}

                    dictionary["key"] = key
                    dictionary["ring"] = self.ring

                    msg[1] = json.dumps(dictionary).encode('utf-8')

                    backend.send_multipart(msg)


                # Forward message to client if it's not a READY
                elif reply != LRU_READY:
                    frontend.send_multipart(msg)

                else: 
                    print("AAAAA")
                    frontend.send_multipart([msg[1], msg[2]])

            if frontend in sockets and sockets[frontend] == zmq.POLLIN:
                msg = frontend.recv_multipart()

                print(msg)
                shoppingList = json.loads(msg[2].decode('utf-8')[3:])
                print(shoppingList)

                if (shoppingList["key"] == None):
                    key = self._hash(shoppingList["url"])
                    shoppingList["key"] = key
                    msg[2] = ("sl:" + json.dumps(shoppingList)).encode('utf-8')
                    print("SHOPPING LIST: ", shoppingList)

                # verificar em que server se guarda

                # workers.pop(0) -> replace por b'<port number>' ou hash key do server
                server_port = str(self.redirect_shopping_list(shoppingList))
                print(server_port)
                server_port_encoded = server_port.encode('utf-8')
                request = [server_port_encoded, msg[1], msg[2], msg[0]]

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