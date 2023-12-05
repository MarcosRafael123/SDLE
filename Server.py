from random import randint
import time
import zmq
import json
import logging
import sys
import random
import re

LRU_READY = "\x01"
SHOPPINGLIST = "sl:"
RING = "ring:"
REP = "sl_rep:"
REPLICATION_FACTOR = 2

class Server: 
    def __init__(self, port): 
        self.key = None
        self.servers = {}
        self.brokerPorts = ["5556"]
        self.port = port 
        self.shopping_lists = []
        self.replicas = {}
        self.context = zmq.Context()
        self.socket = None
        logging.info("Connecting to broker...")
        self.add_to_ring()
        self.run()
        
    def add_to_ring(self):
        worker = self.context.socket(zmq.DEALER)
        worker.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
        worker.connect("tcp://localhost:" + self.brokerPorts[0])
        worker.send(json.dumps("port:" + str(self.port)).encode('utf-8'))
        received = json.loads(worker.recv().decode('utf-8'))
        self.key = received["key"]
        self.servers = received["ring"] # if we want to access key remember it is a string
        print("Server added to ring successfully!")
        print("Server started on port " + str(self.port))
        print("Ring: " + str(self.servers))
        print("Key: " + str(self.key))

    def set_port(self, port):
        self.port = port

    def get_port(self):
        return self.port
    
    def pack_message(self, shoppinglist): 
        dictionary = {}

        dictionary["url"] = shoppinglist.get_url()
        dictionary["items"] = shoppinglist.get_items()

        return json.dumps(dictionary, sort_keys=True)
    
    def unpack_message(self, msg):
        return json.loads(msg[1].decode('utf-8')[3:])
    
    def send_servers_ring(self):
        if len(self.servers) <= 2: 
            return None
        
        random_server = None

        while True:
            random_key = random.choice(list(self.servers.keys()))

            if random_key != "timestamp" and int(random_key) != self.key:
                random_server = self.servers[random_key]
                break

        server = self.context.socket(zmq.DEALER)
        server.connect("tcp://localhost:" + str(random_server))
        server.send(("ring:" + json.dumps(self.servers)).encode('utf-8'))
        
    def get_successors(self, source_key):
        servers_copy = self.servers.copy()
        del servers_copy['timestamp']
        transformed_dict = {int(key): value for key, value in servers_copy.items()}
        transformed_dict = dict(sorted(transformed_dict.items()))
        #print("TRANSFORMED_DICTIONARY_SORTED: ", transformed_dict)

        server_ports = list(transformed_dict.keys())

        print("SERVER_PORTS: ", server_ports)

        try:
            source_index = server_ports.index(source_key)
        except ValueError:
            return []

        num_servers = len(server_ports)
        if num_servers <= 1:
            return []

        replica_servers = []

        for i in range(1, min(REPLICATION_FACTOR + 1, num_servers)):
            index = (source_index + i) % num_servers
            next_server = server_ports[index]
            if next_server != source_key:
                replica_servers.append(next_server)

        return replica_servers
                

    def send_replicas(self, shoppinglist):
        successors = self.get_successors(self.key)

        print("Successors: ", successors)

        for successor in successors:
            server = self.context.socket(zmq.DEALER)
            server.connect("tcp://localhost:" + str(self.servers[str(successor)]))
            print("Sending to server: ", self.servers[str(successor)])
            server.send((REP + json.dumps(shoppinglist) + ":" + str(self.key)).encode('utf-8'))

        return 
    
    def find_lost_shopping_lists_for_server(self):
        servers_copy = self.servers.copy()
        del servers_copy['timestamp']

        hash_range = [int(self.key), self.get_successors(self.key)[0]]

        print("HASH RANGE: ", hash_range)

        lost_shopping_lists = []

        for shopping_list in self.shopping_lists: 
            if not (hash_range[0] <= shopping_list["key"] < hash_range[1]): 
                lost_shopping_lists.append(shopping_list)

        return lost_shopping_lists, hash_range[1]
    
    def transfer_shopping_lists(self):
        lost_shopping_lists, server_to_send = self.find_lost_shopping_lists_for_server()

        server_to_send = self.servers[str(server_to_send)]

        print("LOST SHOPPING LISTS: ", lost_shopping_lists)

        print("BEFORE TRANSFER: ", self.shopping_lists)

        for shopping_list in lost_shopping_lists:
            sl = json.dumps(shopping_list)
            server = self.context.socket(zmq.DEALER)
            server.connect("tcp://localhost:" + str(server_to_send))
            print("Sending to server: ", str(server_to_send))
            server.send(("sl:" + sl).encode('utf-8'))
            self.send_replicas(sl)
            self.shopping_lists.remove(shopping_list)

        print("AFTER TRANSFER: ", self.shopping_lists)

    def run(self):

        worker = self.context.socket(zmq.DEALER)
        worker.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
        worker.connect("tcp://localhost:" + self.brokerPorts[0])
        worker.send_string(LRU_READY)

        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind("tcp://*:" + self.port)

        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        poller.register(worker, zmq.POLLIN)

        while True:
            #start_time = time.time()

            events = dict(poller.poll(timeout=1000))

            if self.socket in events and events[self.socket] == zmq.POLLIN:
                message_received = self.socket.recv_multipart()
        
                if(SHOPPINGLIST in message_received[1].decode('utf-8')):
                    print("Received shopping list")
                    message = message_received[1].decode('utf-8')[3:]
    
                    self.shopping_lists.append(json.loads(message))

                    print("Shopping lists: ", self.shopping_lists)
                    #self.send_replicas(self.shoppinglist)

                if REP in message_received[1].decode('utf-8'):
                    message = message_received[1].decode('utf-8')[7:]

                    print("MESSAGE: ", message)
                    
                    last_colon_index = message.rfind(":")

                    json_part = message[:last_colon_index]
                    key_part = message[last_colon_index + 1:]

                    if key_part.startswith(":"):
                        key_part = key_part[1:]

                    print("KEY_PART: ", key_part)
                    
                    shoppinglist = json.loads(json_part)

                    sls = self.replicas[key_part] if key_part in self.replicas else []
                    sls.append(shoppinglist)
                    self.replicas[key_part] = sls

                    print("Replicas: ", self.replicas)

                if RING in message_received[1].decode('utf-8'):
                    #print("Received ring")

                    message = json.loads(message_received[1].decode('utf-8')[5:])

                    #self.servers = message
                    #print(message)

                    if message["timestamp"] > self.servers["timestamp"]:
                        self.servers = message
                        print("AAA")
                        self.transfer_shopping_lists()
                        print("BBB")

            if worker in events and events[worker] == zmq.POLLIN:
                msg = worker.recv_multipart()

                print(msg)

                message = self.unpack_message(msg)
                print(message)
                
                if(SHOPPINGLIST in msg[1].decode('utf-8')):
                    msg.insert(0, msg[2])
                    msg.pop(3)
                    msg.pop(2)
                    msg[1] = "received shopping list".encode('utf-8')
                    print(msg)
                    worker.send_multipart(msg)
                    self.shopping_lists.append(message)
                    self.send_replicas(message)

                else: 
                    msg.insert(0, msg[2])
                    msg.pop(3)
                    msg.pop(2)
                    msg[1] = "message received".encode('utf-8')
                    print(msg)
                    worker.send_multipart(msg)      
            
            """ elapsed_time = time.time() - start_time

            if elapsed_time < 1:
                time.sleep(1 - elapsed_time) """

            self.send_servers_ring()
            

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python ./Server.py <port>")
        sys.exit(1)

    port = sys.argv[1]
    Server(port)