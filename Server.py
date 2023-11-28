from random import randint
import time
import zmq
import json
import logging
import sys
import random

LRU_READY = "\x01"
SHOPPINGLIST = "sl:"

class Server: 
    def __init__(self, port): 
        self.key = None
        self.servers = {}
        self.brokerPorts = ["5556"]
        self.port = port 
        self.shopping_lists = []
        self.context = zmq.Context()
        self.socket = None
        logging.info("Connecting to broker...")
        self.add_to_ring()
        self.run()
        
    def add_to_ring(self):
        worker = self.context.socket(zmq.REQ)
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
        return json.loads(msg[2].decode('utf-8')[3:])
    
    def send_shopping_list(self, shoppinglist):
        return
    
    def redirect_shopping_list(self, shoppinglist):
        sl_key = shoppinglist["key"]

        my_servers = self.servers.copy()
        del my_servers["timestamp"]

        if len(my_servers) == 1:
            return 2

        min_difference_lower = int('9' * 100)
        min_difference_upper = int('9' * 100)

        min_difference_lower_key = None
        min_difference_upper_key = None

        for key in my_servers:
            if int(key) < sl_key:
                difference = sl_key - int(key)
                print(len(str(difference)))
                if difference < min_difference_lower:
                    min_difference_lower = difference
                    min_difference_lower_key = key
            else:
                difference = int(key) - sl_key
                if difference < min_difference_upper:
                    min_difference_upper = difference
                    min_difference_upper_key = key

        print("Lower: " + str(min_difference_lower_key))   
        print("Upper: " + str(min_difference_upper_key))

        return 
    
    def send_servers_content(self):
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
        server.send(json.dumps(self.servers).encode('utf-8'))
        

    def run(self):

        worker = self.context.socket(zmq.REQ)
        worker.connect("tcp://localhost:" + self.brokerPorts[0])
        worker.send_string(LRU_READY)

        self.send_servers_content()

        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind("tcp://*:" + self.port)

        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        poller.register(worker, zmq.POLLIN)

        while True:
            #events = dict(poller.poll(timeout=100))
            events = dict(poller.poll())

            self.send_servers_content()

            if self.socket in events and events[self.socket] == zmq.POLLIN:
                message = json.loads(self.socket.recv_multipart()[1].decode('utf-8'))

                if message["timestamp"] > self.servers["timestamp"]:
                    self.servers = message
                    print("Updated servers: " + str(self.servers))

            if worker in events and events[worker] == zmq.POLLIN:
                msg = worker.recv_multipart()

                if not msg:
                    break

                message = self.unpack_message(msg)
                print(message)
                
                # check if shopping list belongs in this server
                if(SHOPPINGLIST in msg[2].decode('utf-8')):
                    self.redirect_shopping_list(message)

                    msg[2] = "message received".encode('utf-8')
                    
                    worker.send_multipart(msg)

                else: 
                    msg[2] = "message received".encode('utf-8')
                    worker.send_multipart(msg)                
            

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python ./Server.py <port>")
        sys.exit(1)

    port = sys.argv[1]
    Server(port)