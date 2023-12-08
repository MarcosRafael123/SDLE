from random import randint
import time
import zmq
import json
import logging
import sys
import random
import re
import threading
import random

LRU_READY = "\x01"
SHOPPINGLIST = "sl:"
SHOPPINGLISTS = "sls:"
RING = "ring:"
REP = "sl_rep:"
NREP = "sl_nrep:"
RMREP = "rm_rep:"
REPLICATION_FACTOR = 2
HINTED_HANDOFF_TIMEOUT = 5

class Server: 
    def __init__(self, port): 
        self.key = None
        self.servers = {}
        self.brokerPorts = ["5556"]
        self.port = port 
        self.shopping_lists = []
        self.replicas = {}
        self.hinted_handoff = {}
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
    
    def add_to_shopping_lists(self, shopping_list):
        key_exists = any(item['key'] == shopping_list['key'] for item in self.shopping_lists)

        if not key_exists:
            self.shopping_lists.append(shopping_list)
        else:
            print(f"The key {shopping_list} already exists in the list.")
    
    def clockwise_order(self, key):
        hash_ring = self.servers.copy()
        del hash_ring["timestamp"]

        sorted_keys = sorted(hash_ring.keys())
        try:
            start_index = sorted_keys.index(key)
        except ValueError:
            return []

        rotated_keys = sorted_keys[start_index:] + sorted_keys[:start_index]

        return rotated_keys

    def send_servers_ring(self):
        if len(self.servers) <= 2: 
            return None
        
        random_server = None
        random_key = None

        while True:
            random_key = random.choice(list(self.servers.keys()) + self.brokerPorts)

            if random_key != "timestamp" and int(random_key) != self.key:
                if random_key in self.brokerPorts:
                    random_server = self.brokerPorts[0]
                else:
                    random_server = self.servers[random_key]
                
                break

        #print("Connecting to server: ", random_server)

        server = self.context.socket(zmq.DEALER)
        server.connect("tcp://localhost:" + str(random_server))

        if random_key in self.brokerPorts:
            #server.send_multipart([ str(self.port).encode('utf-8'), ("ring:" + json.dumps(self.servers)).encode('utf-8')])
            server.send_multipart([("ring:" + json.dumps(self.servers)).encode('utf-8')])
        else: 
        #if not (random_key in self.brokerPorts):
            server.send(("ring:" + json.dumps(self.servers)).encode('utf-8'))
        
    def get_successors(self, source_key, num_succ):
        servers_copy = self.servers.copy()
        del servers_copy['timestamp']
        transformed_dict = {int(key): value for key, value in servers_copy.items()}
        transformed_dict = dict(sorted(transformed_dict.items()))

        server_ports = list(transformed_dict.keys())

        try:
            source_index = server_ports.index(source_key)
        except ValueError:
            return []

        num_servers = len(server_ports)
        if num_servers <= 1:
            return []

        replica_servers = []

        for i in range(1, min(num_succ + 1, num_servers)):
            index = (source_index + i) % num_servers
            next_server = server_ports[index]
            if next_server != source_key:
                replica_servers.append(next_server)

        return replica_servers
                

    def send_replicas(self):
        successors = self.get_successors(self.key, REPLICATION_FACTOR)

        for successor in successors:
            server = self.context.socket(zmq.DEALER)
            server.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
            server.connect("tcp://localhost:" + str(self.servers[str(successor)]))
            server.send((REP + json.dumps(self.shopping_lists) + ":" + str(self.key)).encode('utf-8'))

        return 
    
    def find_lost_shopping_lists_for_server(self):
        servers_copy = self.servers.copy()
        del servers_copy['timestamp']

        hash_range = [int(self.key), self.get_successors(self.key, 2)[0]]

        lost_shopping_lists = []

        for shopping_list in self.shopping_lists: 
            if hash_range[0] > hash_range[1]:
                if not (hash_range[0] <= shopping_list["key"] or shopping_list["key"] < hash_range[1]):
                    lost_shopping_lists.append(shopping_list)
            else: 
                if not (hash_range[0] <= shopping_list["key"] < hash_range[1]): 
                    lost_shopping_lists.append(shopping_list)
            
        return lost_shopping_lists, hash_range[1]
    
    def transfer_shopping_lists(self):
        lost_shopping_lists, server_to_send = self.find_lost_shopping_lists_for_server()

        #print("SERVER_TO_SEND: ", server_to_send)
        #print("LOST SHOPPING LISTS: ", lost_shopping_lists)

        server_to_send = self.servers[str(server_to_send)]

        successors = self.get_successors(self.key, 3)

        for shopping_list in lost_shopping_lists:
            sl = json.dumps(shopping_list)
            server = self.context.socket(zmq.DEALER)
            server.connect("tcp://localhost:" + str(server_to_send))
            #print("Sending to server: ", str(server_to_send))
            server.send(("sl:" + sl).encode('utf-8'))
            self.shopping_lists.remove(shopping_list)

        if len(successors) > 2:
            server = self.context.socket(zmq.DEALER)
            server.connect("tcp://localhost:" + str(self.servers[str(successors[2])]))
            #print("Sending to server: ", self.servers[str(successors[1])])
            server.send(("rm_rep:" + str(self.key)).encode('utf-8'))

    def clockwise_order(self, key):
        hash_ring = self.servers.copy()
        del hash_ring["timestamp"]

        sorted_keys = sorted(hash_ring.keys())
        try:
            start_index = sorted_keys.index(key)
        except ValueError:
            return []

        rotated_keys = sorted_keys[start_index:] + sorted_keys[:start_index]

        return rotated_keys


    def process_hinted_handoff(self):
        current_time = time.time()

        # Iterate over hinted_handoff and process entries
        for key, entry in list(self.hinted_handoff.items()):
            elapsed_time = current_time - entry['timestamp']

            if elapsed_time >= HINTED_HANDOFF_TIMEOUT:
                # Handle the case where the timeout has been reached
                print(f"Timeout reached for key {key}. Entry: {entry}")

                order = self.clockwise_order(key)

                # remove from ring
                del self.servers[key]
                self.servers["timestamp"] = time.time()

                print(self.servers)

                # send to broker and to server that will take the changes in to be updated
                if key in self.replicas:
                    self.replicas[key] = self.replicas[key].extend(entry['sls'])
                else:
                    self.replicas[key] = entry['sls']

                #print(self.replicas)

                # send to broker
                server = self.context.socket(zmq.DEALER)
                server.connect("tcp://localhost:" + str(self.brokerPorts[0]))
                server.send(("ring:" + json.dumps(self.servers)).encode('utf-8'))

                # send to server
                server = self.context.socket(zmq.DEALER)
                server.connect("tcp://localhost:" + str(self.servers[order[len(order) - 1]]))
                server.send(("ring:" + json.dumps(self.servers)).encode('utf-8'))
                server.send(("sls:" + json.dumps(self.replicas[key])).encode('utf-8'))

                # Remove the expired timestamp_entry
                del self.hinted_handoff[key]
            else:
                # Your logic to try sending the shopping list to the correct server
                # (e.g., using the information in 'timestamp_entry')
                print(f"Attempting to send shopping lists for key {key}")

                message_to_send = ("sls:" + json.dumps(entry['sls'])).encode('utf-8')

                # Send the message to the server
                self.socket_to_server = self.context.socket(zmq.DEALER)
                self.socket_to_server.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
                self.socket_to_server.connect("tcp://localhost:" + str(self.servers[str(key)]))
                self.socket_to_server.send(message_to_send)

                # Wait for a response with a timeout
                poller = zmq.Poller()
                poller.register(self.socket_to_server, zmq.POLLIN)
                timeout = 1000  # Set your desired timeout in milliseconds

                if self.socket_to_server in dict(poller.poll(timeout=timeout)):
                    # Received a response within the timeout
                    response = self.socket_to_server.recv()
                    print(f"Received response from server: {response}")
                    del self.hinted_handoff[key]
                else:
                    # Handle the case where no response is received within the timeout
                    print("No response received within the timeout.")

                # Cleanup and disconnect
                self.socket_to_server.disconnect("tcp://localhost:" + str(self.servers[str(key)]))
            
    def run(self):

        worker = self.context.socket(zmq.DEALER)
        worker.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
        worker.connect("tcp://localhost:" + self.brokerPorts[0])
        #worker.send_string(LRU_READY)

        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind("tcp://*:" + self.port)

        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        poller.register(worker, zmq.POLLIN)

        while True:

            events = dict(poller.poll(timeout=1000))

            if self.socket in events and events[self.socket] == zmq.POLLIN:
                message_received = self.socket.recv_multipart()
        
                if(SHOPPINGLIST in message_received[1].decode('utf-8')):
                    #print("Received shopping list")
                    message = message_received[1].decode('utf-8')[3:]
    
                    #self.shopping_lists.append(json.loads(message))
                    self.add_to_shopping_lists(json.loads(message))

                    #print("Shopping lists: ", self.shopping_lists)
                    self.send_replicas()

                elif SHOPPINGLISTS in message_received[1].decode('utf-8'):
                    message = message_received[1].decode('utf-8')[4:]

                    print("RECEIVEDDDDD YESSSSSSSSSSSS")
                    print(message)

                    reply_message = "received shopping lists".encode('utf-8')

                    self.socket.send_multipart([message_received[0], reply_message])

                    shopping_lists = json.loads(message)

                    for shopping_list in shopping_lists:
                        print(shopping_list)
                        #self.shopping_lists.append(shopping_list)
                        self.add_to_shopping_lists(shopping_list)

                    print("Shopping lists: ", self.shopping_lists)

                    self.send_replicas()

                elif REP in message_received[1].decode('utf-8'):
                    message = message_received[1].decode('utf-8')[7:]

                    print("MESSAGE: ", message)
                    
                    last_colon_index = message.rfind(":")

                    json_part = message[:last_colon_index]
                    key_part = message[last_colon_index + 1:]

                    if key_part.startswith(":"):
                        key_part = key_part[1:]

                    print("KEY_PART: ", key_part)
                    
                    replica = json.loads(json_part)

                    self.replicas[key_part] = replica

                    print("Replicas: ", self.replicas.keys())

                elif RMREP in message_received[1].decode('utf-8'):
                    message = message_received[1].decode('utf-8')[7:]

                    print("MESSAGE: ", message)

                    if message in self.replicas.keys():
                        del self.replicas[message]
                        print("Replicas after deletion: ", self.replicas.keys())
 
                    print("YAU")

                elif RING in message_received[1].decode('utf-8'):
                    message_reply = "received ring".encode('utf-8')

                    self.socket.send_multipart([message_received[0], message_reply])

                    message = json.loads(message_received[1].decode('utf-8')[5:])

                    if message["timestamp"] > self.servers["timestamp"]:
                        self.servers = message
                        self.transfer_shopping_lists()
                        self.send_replicas()

            if worker in events and events[worker] == zmq.POLLIN:
                msg = worker.recv_multipart()

                print(msg)
                
                if(SHOPPINGLIST in msg[0].decode('utf-8')):
                    response = "received shopping list".encode('utf-8')
                    print(msg)
                    worker.send_multipart([msg[1], response])

                    message = msg[0].decode('utf-8')[3:]

                    last_colon_index = message.rfind(":")

                    json_part = message[:last_colon_index]
                    key_part = message[last_colon_index + 1:]

                    if key_part.startswith(":"):
                        key_part = key_part[1:]

                    print("KEY_PART: ", key_part)
                    print("JSON_PART: ", json_part)

                    if key_part == str(self.key):
                        print("BElongs here")
                        self.add_to_shopping_lists(json.loads(json_part))
                        self.send_replicas()
                    else: 
                        print("Goes to hinted handoff")

                        if not (key_part in self.hinted_handoff):
                            self.hinted_handoff[key_part] = {'timestamp': time.time(), 'sls': [json.loads(json_part)]}
                            
                        else: 
                            self.hinted_handoff[key_part]['sls'] = self.hinted_handoff[key_part]['sls'].append(json.loads(json_part))
                    

                else: 
                    msg.insert(0, msg[2])
                    msg.pop(3)
                    msg.pop(2)
                    msg[1] = "message received".encode('utf-8')
                    print(msg)
                    worker.send_multipart(msg)      
    

            self.send_servers_ring()
            self.process_hinted_handoff()

            #print("Ring: ", self.servers.keys())
            #print("Replicas: ", self.replicas)
            #print("Hinted handoff: ", self.hinted_handoff)
            

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python ./Server.py <port>")
        sys.exit(1)

    port = sys.argv[1]
    Server(port)