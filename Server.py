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
import ShoppingListCRDT
import sqlite3

LRU_READY = "\x01"
SHOPPINGLIST = "sl:"
SHOPPINGLISTS = "sls:"
RING = "ring:"
REQUEST_SHOPPING_LIST = "requestSL:"
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
        self.load_schema()
        self.run()
        
    def add_to_ring(self):
        worker = self.context.socket(zmq.DEALER)
        worker.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
        worker.connect("tcp://localhost:" + self.brokerPorts[0])
        worker.send(json.dumps("port:" + str(self.port)).encode('utf-8'))
        received = json.loads(worker.recv().decode('utf-8'))
        self.key = received["key"]
        self.servers = received["ring"]
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
            return 1
        else:
            print(f"The key {shopping_list} already exists in the list.")
            sl = ShoppingListCRDT.ShoppingListCRDT(shopping_list['url'], shopping_list['additions'], shopping_list['removals'])
            sl.set_key(shopping_list['key'])

            index = next((i for i, item in enumerate(self.shopping_lists) if item['key'] == shopping_list['key']), None)

            if index is not None:
                sl_original = ShoppingListCRDT.ShoppingListCRDT(self.shopping_lists[index]['url'], self.shopping_lists[index]['additions'], self.shopping_lists[index]['removals'])
                sl_original.set_key(self.shopping_lists[index]['key'])

                sl_merged = sl.merge(sl_original)

                self.shopping_lists[index]['items'] = sl_merged.get_items()
                self.shopping_lists[index]['additions'] = sl_merged.get_additions()
                self.shopping_lists[index]['removals'] = sl_merged.get_removals()

            return 2

    
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

        server = self.context.socket(zmq.DEALER)
        server.connect("tcp://localhost:" + str(random_server))

        if random_key in self.brokerPorts:
            server.send_multipart([("ring:" + json.dumps(self.servers)).encode('utf-8')])
        else: 
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

        server_to_send = self.servers[str(server_to_send)]

        successors = self.get_successors(self.key, 3)

        for shopping_list in lost_shopping_lists:
            sl = json.dumps(shopping_list)
            server = self.context.socket(zmq.DEALER)
            server.connect("tcp://localhost:" + str(server_to_send))
            server.send(("sl:" + sl).encode('utf-8'))
            self.shopping_lists.remove(shopping_list)

            connection = sqlite3.connect('server' + self.port + '.db')
            cursor = connection.cursor()

            cursor.execute("DELETE FROM ShoppingListsServers WHERE server_port = ?",
                        (self.port,))
            
            cursor.execute("DELETE FROM ItemsServers WHERE shopping_list_servers_id = ?",
                        (shopping_list["url"],))

            connection.commit()
            connection.close()

        if len(successors) > 2:
            server = self.context.socket(zmq.DEALER)
            server.connect("tcp://localhost:" + str(self.servers[str(successors[2])]))
            server.send(("rm_rep:" + str(self.key)).encode('utf-8'))

    def clockwise_order(self, key):
        hash_ring = self.servers.copy()
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


    def process_hinted_handoff(self):
        current_time = time.time()

        for key, entry in list(self.hinted_handoff.items()):
            elapsed_time = current_time - entry['timestamp']

            if elapsed_time >= HINTED_HANDOFF_TIMEOUT:
                print(f"Timeout reached for key {key}. Entry: {entry}")

                order = self.clockwise_order(key)

                del self.servers[key]
                self.servers["timestamp"] = time.time()

                if key in self.replicas:
                    existing_entries = {item['url']: item for item in self.replicas[key]}
                    if entry['sls'] is not None:
                        for new_entry in entry['sls']:
                            url = new_entry['url']

                            if url in existing_entries:
                                old = ShoppingListCRDT.ShoppingListCRDT(existing_entries[url]['url'], existing_entries[url]['additions'], existing_entries[url]['removals'])
                                old.set_key(existing_entries[url]['key'])

                                new = ShoppingListCRDT.ShoppingListCRDT(new_entry['url'], new_entry['additions'], new_entry['removals'])
                                new.set_key(new_entry['key'])

                                merged = old.merge(new)

                                existing_entries[url]['items'] = merged.get_items()
                                existing_entries[url]['additions'] = merged.get_additions()
                                existing_entries[url]['removals'] = merged.get_removals()

                            else:
                                self.replicas[key].append(new_entry)

                else: 
                    self.replicas[key] = entry['sls']


                server = self.context.socket(zmq.DEALER)
                server.connect("tcp://localhost:" + str(self.brokerPorts[0]))
                server.send_multipart([("ring:" + json.dumps(self.servers)).encode('utf-8')])

                server = self.context.socket(zmq.DEALER)
                server.connect("tcp://localhost:" + str(self.servers[order[len(order) - 1]]))
                server.send(("ring:" + json.dumps(self.servers)).encode('utf-8'))
                server.send(("sls:" + json.dumps(self.replicas[key])).encode('utf-8'))

                del self.hinted_handoff[key]
            else:
                print(f"Attempting to send shopping lists for key {key}")

                message_to_send = ("sls:" + json.dumps(entry['sls'])).encode('utf-8')

                self.socket_to_server = self.context.socket(zmq.DEALER)
                self.socket_to_server.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
                self.socket_to_server.connect("tcp://localhost:" + str(self.servers[str(key)]))
                self.socket_to_server.send(message_to_send)

                poller = zmq.Poller()
                poller.register(self.socket_to_server, zmq.POLLIN)
                timeout = 1000

                if self.socket_to_server in dict(poller.poll(timeout=timeout)):
                    response = self.socket_to_server.recv()
                    print(f"Received response from server: {response}")
                    del self.hinted_handoff[key]
                else:
                    print("No response received within the timeout.")

                self.socket_to_server.disconnect("tcp://localhost:" + str(self.servers[str(key)]))
            
    def run(self):

        worker = self.context.socket(zmq.DEALER)
        worker.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
        worker.connect("tcp://localhost:" + self.brokerPorts[0])

        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind("tcp://*:" + self.port)

        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        poller.register(worker, zmq.POLLIN)

        while True:

            events = dict(poller.poll(timeout=1000))

            if self.socket in events and events[self.socket] == zmq.POLLIN:
                message_received = self.socket.recv_multipart()
                print("MESSAGE RECEIVED: ", message_received)
        
                if(SHOPPINGLIST in message_received[1].decode('utf-8')):
                    message = message_received[1].decode('utf-8')[3:]
    
                    self.add_to_shopping_lists(json.loads(message))

                    self.send_replicas()

                    if json.loads(message)["items"] == {}:
                        connection = sqlite3.connect('server' + self.port + '.db')
                        cursor = connection.cursor()

                        cursor.execute("SELECT COUNT(*) FROM ShoppingListsServers WHERE url = ?", (json.loads(message)["url"],))
                        result = cursor.fetchone()

                        if result[0] == 0:
                            cursor.execute("INSERT INTO ShoppingListsServers (server_port, url, key) VALUES (?, ?, ?)",
                                        (self.port, json.loads(message)["url"], str(json.loads(message)["key"])))
                        connection.commit()
                        connection.close()
                    else:
                        for key, value in json.loads(message)["items"].items():
                            connection = sqlite3.connect('server' + self.port + '.db')
                            cursor = connection.cursor()

                            cursor.execute("SELECT COUNT(*) FROM ShoppingListsServers WHERE url = ?", (json.loads(message)["url"],))
                            result = cursor.fetchone()
                            if result[0] == 0:
                                cursor.execute("INSERT INTO ShoppingListsServers (server_port, url, key) VALUES (?, ?, ?)",
                                            (self.port, json.loads(message)["url"], str(json.loads(message)["key"])))

                            cursor.execute("SELECT COUNT(*) FROM ItemsServers WHERE shopping_list_servers_id = ? AND name = ?",
                                        (json.loads(message)["url"], key))
                            result = cursor.fetchone()
                            if result[0] > 0:
                                if value > 0:
                                    cursor.execute("UPDATE ItemsServers SET quantity = ? WHERE shopping_list_servers_id = ? AND name = ?",
                                        (value, json.loads(message)["url"], key))
                                else:
                                    cursor.execute("DELETE FROM ItemsServers WHERE shopping_list_servers_id=? AND name=?", (json.loads(message)["url"], key))

                            else:
                                if value > 0:
                                    cursor.execute("INSERT INTO ItemsServers (shopping_list_servers_id, name, quantity) VALUES (?, ?, ?)",
                                        (json.loads(message)["url"], key, value))

                            connection.commit()
                            connection.close()

                elif SHOPPINGLISTS in message_received[1].decode('utf-8'):
                    message = message_received[1].decode('utf-8')[4:]

                    print(message)

                    reply_message = "received shopping lists".encode('utf-8')

                    self.socket.send_multipart([message_received[0], reply_message])

                    shopping_lists = json.loads(message)

                    for shopping_list in shopping_lists:
                        self.add_to_shopping_lists(shopping_list)

                    self.send_replicas()

                elif REP in message_received[1].decode('utf-8'):
                    message = message_received[1].decode('utf-8')[7:]

                    print("MESSAGE: ", message)
                    
                    last_colon_index = message.rfind(":")

                    json_part = message[:last_colon_index]
                    key_part = message[last_colon_index + 1:]

                    if key_part.startswith(":"):
                        key_part = key_part[1:]
                    
                    replica = json.loads(json_part)

                    self.replicas[key_part] = replica

                elif RMREP in message_received[1].decode('utf-8'):
                    message = message_received[1].decode('utf-8')[7:]

                    print("MESSAGE: ", message)

                    if message in self.replicas.keys():
                        del self.replicas[message] 

                elif RING in message_received[1].decode('utf-8'):
                    message_reply = "received ring".encode('utf-8')

                    self.socket.send_multipart([message_received[0], message_reply])

                    message = json.loads(message_received[1].decode('utf-8')[5:])

                    if message["timestamp"] > self.servers["timestamp"]:
                        self.servers = message
                        self.send_servers_ring()    
                        self.transfer_shopping_lists()
                        self.send_replicas()

            if worker in events and events[worker] == zmq.POLLIN:
                msg = worker.recv_multipart()

                if REQUEST_SHOPPING_LIST in msg[0].decode('utf-8'): 
                    message = msg[0].decode('utf-8')[10:]

                    print("MESSAGE: ", message) 

                    sl = None

                    for shopping_list in self.shopping_lists:
                        if shopping_list["url"] == message: 
                            sl = shopping_list

                    if sl == None: 
                        for server in self.replicas.keys(): 
                            for shoppinglist in self.replicas[server]: 
                                if shoppinglist["url"] == message: 
                                    sl = shoppinglist

                    if sl == None:
                        sl = "Shopping list not found"
                    
                    worker.send_multipart([msg[1], json.dumps(sl).encode('utf-8')])
                
                elif(SHOPPINGLIST in msg[0].decode('utf-8')):
                    message = msg[0].decode('utf-8')[3:]

                    print("MESSAGE: ", message)

                    last_colon_index = message.rfind(":")

                    json_part = message[:last_colon_index]
                    key_part = message[last_colon_index + 1:]

                    if key_part.startswith(":"):
                        key_part = key_part[1:]

                    if key_part == str(self.key):
                        print("Belongs here")

                        sl = json.loads(json_part)

                        state = self.add_to_shopping_lists(sl)

                        if state == 2: 
                            response = "already exists".encode('utf-8')
                        else: 
                            response = "received shopping list".encode('utf-8')

                        worker.send_multipart([msg[1], response])

                        self.send_replicas()

                        if json.loads(json_part)["items"] == {}:
                            connection = sqlite3.connect('server' + self.port + '.db')
                            cursor = connection.cursor()

                            cursor.execute("SELECT COUNT(*) FROM ShoppingListsServers WHERE url = ?", (json.loads(json_part)["url"],))
                            result = cursor.fetchone()

                            if result[0] == 0:
                                cursor.execute("INSERT INTO ShoppingListsServers (server_port, url, key) VALUES (?, ?, ?)",
                                            (self.port, json.loads(json_part)["url"], str(json.loads(json_part)["key"])))

                            connection.commit()
                            connection.close()
                        else:
                            for key, value in json.loads(json_part)["items"].items():
                                connection = sqlite3.connect('server' + self.port + '.db')
                                cursor = connection.cursor()

                                cursor.execute("SELECT COUNT(*) FROM ItemsServers WHERE shopping_list_servers_id = ? AND name = ?",
                                            (json.loads(json_part)["url"], key))
                                result = cursor.fetchone()
                                if result[0] > 0:
                                    if value > 0:
                                        cursor.execute("UPDATE ItemsServers SET quantity = ? WHERE shopping_list_servers_id = ? AND name = ?",
                                            (value, json.loads(json_part)["url"], key))
                                    else:
                                        cursor.execute("DELETE FROM ItemsServers WHERE shopping_list_servers_id=? AND name=?", (json.loads(json_part)["url"], key))

                                else:
                                    if value > 0:
                                        cursor.execute("INSERT INTO ItemsServers (shopping_list_servers_id, name, quantity) VALUES (?, ?, ?)",
                                            (json.loads(json_part)["url"], key, value))

                                connection.commit()
                                connection.close()

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
                    worker.send_multipart(msg)      
    

            self.send_servers_ring()
            self.process_hinted_handoff()

    def load_schema(self):
        connection = sqlite3.connect('server' + self.port + '.db')
        cursor = connection.cursor()

        with open('database/shopping_lists.sql', 'r') as sql_file:
            sql_script = sql_file.read()
            cursor.executescript(sql_script)
        
        cursor.execute("SELECT id FROM Servers WHERE key=? AND port=?", (str(self.key), self.port))
        existing_server = cursor.fetchone()

        if existing_server:
            print("Connecting to existing server...")
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