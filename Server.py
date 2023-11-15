from random import randint
import time
import zmq
import json

LRU_READY = "\x01"

class Server: 
    def __init__(self, port): 
        self.port = port 
        self.shopping_lists = []

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
        
    
    def run(self):

        context = zmq.Context()
        worker = context.socket(zmq.REQ)

        worker.connect("tcp://localhost:" + self.port)

        worker.send_string(LRU_READY)

        while True:
            msg = worker.recv_multipart()

            if not msg:
                break

            print(self.unpack_message(msg))

            msg[2] = "message received".encode('utf-8')

            worker.send_multipart(msg)
            
def main():
    portBackend = "5556"

    server = Server(portBackend)

    server.run()

if __name__ == "__main__":
    main()