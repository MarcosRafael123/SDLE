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

        worker = self.context.socket(zmq.DEALER)
        worker.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
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
                message_received = self.socket.recv_multipart()
                print(message_received)
                print(message_received[1])
                if(SHOPPINGLIST in message_received[1].decode('utf-8')):
                    print(self.key)

                
                message = json.loads(self.socket.recv_multipart()[1].decode('utf-8'))

                if message["timestamp"] > self.servers["timestamp"]:
                    self.servers = message
                    print("Updated servers: " + str(self.servers))

            if worker in events and events[worker] == zmq.POLLIN:
                msg = worker.recv_multipart()

                print(msg)

                message = self.unpack_message(msg)
                print(message)
                
                # check if shopping list belongs in this server
                if(SHOPPINGLIST in msg[1].decode('utf-8')):
                    msg.insert(0, msg[2])
                    msg.pop(3)
                    msg.pop(2)
                    msg[1] = "received shopping list".encode('utf-8')
                    print(msg)
                    worker.send_multipart(msg)

                else: 
                    msg.insert(0, msg[2])
                    msg.pop(3)
                    msg.pop(2)
                    msg[1] = "message received".encode('utf-8')
                    print(msg)
                    worker.send_multipart(msg)                
            

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python ./Server.py <port>")
        sys.exit(1)

    port = sys.argv[1]
    Server(port)