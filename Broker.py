import zmq
import hashlib
import json
import time

LRU_READY = "\x01"
PORT = "port:"
RING = "ring:"
SHOPPING_LIST = "sl:"
REQUEST_SHOPPING_LIST = "requestSL:"

class Broker: 
    def __init__(self, portFrontend, portBackend, nodes=None, hash_func = hashlib.sha256): 
        self.portFrontend = portFrontend
        self.portBackend = portBackend
        self.hash_func = hash_func
        self.ring = {}
        self.sorted_keys = []

    def redirect_shopping_list(self, shoppinglist):
        sl_key = shoppinglist["key"]

        my_servers = self.ring.copy()
        print("RING CONTENT: ", self.ring)
        del my_servers["timestamp"]
        print("MY_SERVERS: ", my_servers)
        my_servers = dict(sorted(my_servers.items(), reverse=True))
        print("MY_SERVERS: ", my_servers)

        for key, value in my_servers.items():
            print("KEY: ", key)
            print("SL_KEY: ", sl_key)
            
            if int(key) <= sl_key:
                server_to_send = value
                return key, server_to_send

        max_key = max(my_servers, key=my_servers.get)
        return max_key, my_servers[max_key]

    def request_shopping_list(self, url):
        sl_key = self._hash(url)

        my_servers = self.ring.copy()
        print("RING CONTENT: ", self.ring)
        del my_servers["timestamp"]
        print("MY_SERVERS: ", my_servers)
        my_servers = dict(sorted(my_servers.items(), reverse=True))
        print("MY_SERVERS: ", my_servers)

        for key, value in my_servers.items():
            print("KEY: ", key)
            print("SL_KEY: ", sl_key)
            
            if int(key) <= sl_key:
                server_to_send = value
                return key, server_to_send

        max_key = max(my_servers, key=my_servers.get)
        return max_key, my_servers[max_key]

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

                print("BACKEND MESSAGE RECEIVED:")
                reply = msg[1]
                
                if (PORT in reply.decode('utf-8')):
                    key = self.add_node(reply.decode('utf-8').split(':')[1].strip('"'))

                    message = str(key)

                    print(message)

                    dictionary = {}

                    dictionary["key"] = key
                    dictionary["ring"] = self.ring

                    msg[1] = json.dumps(dictionary).encode('utf-8')

                    backend.send_multipart(msg)

                if RING in msg[1].decode('utf-8'):
                    print("Received ring")

                    received = json.loads(reply.decode('utf-8')[5:])
                    ring_received = {int(key): value if key != 'timestamp' else value for key, value in received.items() if key != 'timestamp'}
                    ring_received["timestamp"] = received["timestamp"]

                    print(ring_received)

                    if(self.ring['timestamp'] < ring_received['timestamp']):
                        self.ring = ring_received
                        print(self.ring)
                        

            if frontend in sockets and sockets[frontend] == zmq.POLLIN:
                msg = frontend.recv_multipart()

                print(msg)

                if REQUEST_SHOPPING_LIST in msg[1].decode('utf-8'): 
                    server_key, server_port = self.request_shopping_list(msg[1].decode('utf-8')[10:])
                    print("SERVER TO REQUEST: ", server_key, ", ", server_port)

                    server_port = str(server_port)                      

                    next_servers = self.clockwise_order(int(server_key))
                    print("NEXT SERVERS: ", next_servers)

                    start_time = time.time()
                    timeout = 2

                    success = False

                    for server_hash in next_servers:
                        server_port_encoded = str(self.ring[server_hash]).encode('utf-8')
                        request = [server_port_encoded, msg[1], msg[0]]

                        while True:
                            backend.send_multipart(request)
                            sockets = dict(poll_both.poll(timeout=100))

                            if backend in sockets and sockets[backend] == zmq.POLLIN:
                                response_msg = backend.recv_multipart()

                                print("Response received:", response_msg)

                                if len(response_msg) == 2 and (RING in response_msg[1].decode('utf-8') or PORT in response_msg[1].decode('utf-8')):
                                    continue

                                if len(response_msg) == 3:
                                    frontend.send_multipart([response_msg[1], response_msg[2]])
                                else:
                                    frontend.send_multipart(response_msg)

                                success = True
                                break

                            elapsed_time = time.time() - start_time

                            if elapsed_time >= timeout:
                                print("Timeout reached. No response received.")
                                break

                        if success:
                            break 

                
                elif SHOPPING_LIST in msg[2].decode('utf-8'):
                    shoppingList = json.loads(msg[2].decode('utf-8')[3:])
                    print(shoppingList)

                    if (shoppingList["key"] == None):
                        key = self._hash(shoppingList["url"])
                        shoppingList["key"] = key
                        print("SHOPPING LIST: ", shoppingList)

                    server_key, server_port = self.redirect_shopping_list(shoppingList)
                    server_port = str(server_port)
                    print(server_port)

                    msg[2] = ("sl:" + json.dumps(shoppingList) + ":" + str(server_key)).encode('utf-8')

                    print("SERVER KEY: ", server_key)

                    next_servers = self.clockwise_order(int(server_key))
                    print("NEXT SERVERS: ", next_servers)

                    print("RING STATE: ", self.ring)

                    start_time = time.time()
                    timeout = 2

                    success = False

                    for server_hash in next_servers:
                        server_port_encoded = str(self.ring[server_hash]).encode('utf-8')
                        request = [server_port_encoded, msg[2], msg[0]]

                        while True:
                            backend.send_multipart(request)
                            sockets = dict(poll_both.poll(timeout=100))

                            if backend in sockets and sockets[backend] == zmq.POLLIN:
                                response_msg = backend.recv_multipart()

                                print("Response received:", response_msg)

                                if len(response_msg) == 2 and (RING in response_msg[1].decode('utf-8') or PORT in response_msg[1].decode('utf-8')):
                                    continue

                                if len(response_msg) == 3:
                                    frontend.send_multipart([response_msg[1], response_msg[2]])
                                else:
                                    frontend.send_multipart(response_msg)

                                success = True
                                break

                            elapsed_time = time.time() - start_time

                            if elapsed_time >= timeout:
                                print("Timeout reached. No response received.")
                                break

                        if success:
                            break 

                    if not success:
                        print("No servers available. Message lost.")
                        print(msg)
                        msg[2] = "shopping list not saved".encode('utf-8')
                        frontend.send_multipart(msg)


            print("RING ESTADO: ", self.ring)
    

    def _hash(self, key):
        return int(self.hash_func(str(key).encode()).hexdigest(), 16)
    
    def clockwise_order(self, key):
        hash_ring = self.ring.copy()
        del hash_ring["timestamp"]

        sorted_keys = sorted(hash_ring.keys())
        
        if len(sorted_keys) <= 1:
            return sorted_keys

        try:
            start_index = sorted_keys.index(key)
        except ValueError:
            return []

        rotated_keys = sorted_keys[start_index:] + sorted_keys[:start_index]

        return rotated_keys


    def add_node(self, node):
        key = self._hash(f"{node}")
        self.ring[key] = int(node)
        self.ring["timestamp"] = time.time()
        self.sorted_keys.append(key)
        self.sorted_keys.sort()
        print(self.ring)
        return key        
        

def main():
    portFrontend = "5555"
    portBackend = "5556"

    broker = Broker(portFrontend, portBackend)

    broker.run()

if __name__ == "__main__":
    main()