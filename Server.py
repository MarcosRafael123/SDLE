from random import randint
import time
import zmq
import json
import logging
import sys

LRU_READY = "\x01"

class Server: 
    def __init__(self, port): 
        self.key = None
        self.servers = {}
        self.brokerPorts = ["5556"]
        self.port = port 
        self.shopping_lists = []
        self.context = zmq.Context()
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
        return json.loads(msg[2])
    
    def send_shopping_list(self, shoppinglist):
        return
    
    def run(self):

        worker = self.context.socket(zmq.REQ)

        worker.connect("tcp://localhost:" + self.brokerPorts[0])

        worker.send_string(LRU_READY)

        while True:
            msg = worker.recv_multipart()

            if not msg:
                break

            print(self.unpack_message(msg))

            msg[2] = "message received".encode('utf-8')

            worker.send_multipart(msg)
            

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python ./Server.py <port>")
        sys.exit(1)

    port = sys.argv[1]
    Server(port)