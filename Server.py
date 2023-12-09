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
import sqlite3

LRU_READY = "\x01"
SHOPPINGLIST = "sl:"
RING = "ring:"
REP = "sl_rep:"
NREP = "sl_nrep:"
RMREP = "rm_rep:"
REPLICATION_FACTOR = 2

class Server: 
    def __init__(self, port): 
        self.key = None
        self.lock = threading.Lock()
        self.servers = {}
        self.brokerPorts = ["5556"]
        self.port = port 
        self.shopping_lists = []
        self.replicas = {}
        self.context = zmq.Context()
        self.socket = None
        logging.info("Connecting to broker...")
        self.add_to_ring()
        self.load_schema()
        self.send_servers_ring()
        print("Ring: ", self.servers.keys())
        #threading.Thread(target=self.send_servers_ring).start()
        threading.Thread(target=self.run).start()
        
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
        print("SENDING SERVERS RING")
        with self.lock:
            if len(self.servers) <= 2: 
                return None
            
            random_server = None
            random_key = None

            while True:
                random_key = random.choice(list(self.servers.keys()))# + self.brokerPorts)

                if random_key != "timestamp" and int(random_key) != self.key:
                    """ if random_key in self.brokerPorts:
                        random_server = self.brokerPorts[0]
                    else:
                        random_server = self.servers[random_key] """
                    random_server = self.servers[random_key]
                    
                    break

            server = self.context.socket(zmq.DEALER)
            server.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
            server.connect("tcp://localhost:" + str(random_server))
            server.send(("ring:" + json.dumps(self.servers)).encode('utf-8'))

            poller = zmq.Poller()
            poller.register(server, zmq.POLLIN)

            socks = dict(poller.poll(timeout=5000))  # 1000 milliseconds (1 second) timeout

            if server in socks and socks[server] == zmq.POLLIN:
                # Received a reply within the timeout
                reply = server.recv()
                #print("Received reply:", reply.decode('utf-8'))
                # Process the reply if needed
            else:
                del self.servers[random_key]
                self.servers["timestamp"] = time.time()

            server.close()
        
    def get_successors(self, source_key, num_succ):
        servers_copy = self.servers.copy()
        print("SERVERS_COPY HERE: ", servers_copy)
        del servers_copy['timestamp']
        transformed_dict = {int(key): value for key, value in servers_copy.items()}
        transformed_dict = dict(sorted(transformed_dict.items()))
        #print("TRANSFORMED_DICTIONARY_SORTED: ", transformed_dict)

        server_ports = list(transformed_dict.keys())

        #print("SERVER_PORTS: ", server_ports)

        try:
            source_index = server_ports.index(source_key)
        except ValueError:
            return []

        num_servers = len(server_ports)
        print(num_servers)
        if num_servers <= 1:
            return []
        
        replica_servers = []

        print("reached here 0")
        for i in range(1, min(num_succ + 1, num_servers)):
            index = (source_index + i) % num_servers
            next_server = server_ports[index]
            print("reached here 1")
            if next_server != source_key:
                print("reached here 2")
                replica_servers.append(next_server)

        return replica_servers
                

    def send_replicas(self):
        successors = self.get_successors(self.key, REPLICATION_FACTOR)
        print("SUCCCCCCCCCCCCCCCCCCCCCCCCCESSSSSSSSSSSSSSSSSSSSSSSSSSSORS: ", successors)

        for successor in successors:
            server = self.context.socket(zmq.DEALER)
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
                    #print("Received shopping list")
                    message = message_received[1].decode('utf-8')[3:]
    
                    self.shopping_lists.append(json.loads(message))

                    #print("Shopping lists: ", self.shopping_lists)
                    self.send_replicas()

                if REP in message_received[1].decode('utf-8'):
                    message = message_received[1].decode('utf-8')[7:]

                    print("MESSAGE: ", message)
                    
                    last_colon_index = message.rfind(":")

                    json_part = message[:last_colon_index]
                    key_part = message[last_colon_index + 1:]

                    if key_part.startswith(":"):
                        key_part = key_part[1:]

                    print("KEY_PART: ", key_part)
                    print("SERVERS DICTIONARY", self.servers)
                    
                    replica = json.loads(json_part)

                    self.replicas[key_part] = replica

                    print("Replica: ", replica)
                    if replica != []:
                        # Insert the shopping list into the database with the corresponding client ID
                        connection = sqlite3.connect('server' + self.port + '.db')
                        cursor = connection.cursor()

                        # Insert the shopping list with the associated client ID
                        cursor.execute("INSERT INTO Replicas (id, server_sender, url, key, timestamp) VALUES (?, ?, ?, ?, ?)",
                                    (replica[0]["uuid"], self.servers[key_part], replica[0]["url"], str(replica[0]["key"]), replica[0]["timestamp"]))

                        connection.commit()
                        connection.close()


                if RMREP in message_received[1].decode('utf-8'):
                    message = message_received[1].decode('utf-8')[7:]

                    print("MESSAGE: ", message)

                    if message in self.replicas.keys():
                        del self.replicas[message]
                        print("Replicas after deletion: ", self.replicas.keys())
 
                    print("YAU")

                if RING in message_received[1].decode('utf-8'):
                    with self.lock:
                        #print("Received ring")

                        message_reply = "received ring".encode('utf-8')

                        self.socket.send_multipart([message_received[0], message_reply])

                        message = json.loads(message_received[1].decode('utf-8')[5:])

                        if message["timestamp"] > self.servers["timestamp"]:
                            self.servers = message
                            self.transfer_shopping_lists()
                            self.send_replicas()

            if worker in events and events[worker] == zmq.POLLIN:
                msg = worker.recv_multipart()

                message = self.unpack_message(msg)
                print(message)
                
                if(SHOPPINGLIST in msg[1].decode('utf-8')):
                    print("MSGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG: ", message["items"])
                    if (message["items"] == {}):
                        msg.insert(0, msg[2])
                        msg.pop(3)
                        msg.pop(2)
                        msg[1] = "received shopping list".encode('utf-8')
                        print(msg)
                        worker.send_multipart(msg)
                        self.shopping_lists.append(message)
                        print("MESSAGEEEEEEEEEEEEE: ", message)
                        # Insert the shopping list into the database with the corresponding client ID
                        connection = sqlite3.connect('server' + self.port + '.db')
                        cursor = connection.cursor()

                        # Insert the shopping list with the associated client ID
                        cursor.execute("INSERT INTO ShoppingListsServers (id, server_port, url, key, timestamp) VALUES (?, ?, ?, ?, ?)",
                                    (message["uuid"], self.port, message["url"], str(message["key"]), message["timestamp"]))

                        connection.commit()
                        connection.close()
                        print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                        self.send_replicas()
                    else:
                        msg.insert(0, msg[2])
                        msg.pop(3)
                        msg.pop(2)
                        msg[1] = "received shopping list update".encode('utf-8')
                        print(msg)
                        worker.send_multipart(msg)
                        self.shopping_lists.append(message)
                        print("MESSAGEEEEEEEEEEEEE: ", message)
                        # Insert the shopping list into the database with the corresponding client ID
                        connection = sqlite3.connect('server' + self.port + '.db')
                        cursor = connection.cursor()

                        # Insert the shopping list with the associated client ID
                        print("MESSAGE ITEMS= ", message["items"])
                        for key, value in message["items"].items():
                            print("Key:", key)
                            print("Value:", value)
                            cursor.execute("SELECT COUNT(*) FROM ItemsServers WHERE shopping_list_servers_id = ? AND name = ?",
                                           (message["uuid"], key))
                            result = cursor.fetchone()
                            if result[0] > 0:
                                if value > 0:
                                    cursor.execute("UPDATE ItemsServers SET quantity = ? WHERE shopping_list_servers_id = ? AND name = ?",
                                        (value, message["uuid"], key))
                                else:
                                    print(key)
                                    print("DELETING ITEM")
                                    cursor.execute("DELETE FROM ItemsServers WHERE shopping_list_servers_id=? AND name=?", (message["uuid"], key))

                            else:
                                # Record doesn't exist, perform an INSERT
                                if value > 0:
                                    cursor.execute("INSERT INTO ItemsServers (shopping_list_servers_id, name, quantity) VALUES (?, ?, ?)",
                                        (message["uuid"], key, value))

                        connection.commit()
                        connection.close()
                        self.send_replicas()

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

            """ self.send_servers_ring()
            print("Ring: ", self.servers.keys()) """


    def load_schema(self):
        # Connect to the SQLite database
        connection = sqlite3.connect('server' + self.port + '.db')
        cursor = connection.cursor()

        # Read and execute the SQL file
        with open('database/shopping_lists.sql', 'r') as sql_file:
            print("EWEWREWFWEFEFREGH")
            sql_script = sql_file.read()
            cursor.executescript(sql_script)
        
        # Check if the client already exists
        print("KEY: ", type(self.key))
        print("PORT: ", self.port)
        cursor.execute("SELECT id FROM Servers WHERE key=? AND port=?", (str(self.key), self.port))
        existing_server = cursor.fetchone()
        print(existing_server)

        if existing_server:
            print("Connecting to existing server...")
            """ print(self.key)
            print(self.port)
            cursor.execute("SELECT * FROM ShoppingListsServers WHERE server_port=?", (self.port,))
            sls = cursor.fetchall()
            print(sls) """
        else:
            cursor.execute("INSERT INTO Servers (key, port) VALUES (?, ?)", (str(self.key), self.port))

        connection.commit()
        connection.close()
            

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python ./Server.py <port>")
        sys.exit(1)

    port = sys.argv[1]
    Server(port)