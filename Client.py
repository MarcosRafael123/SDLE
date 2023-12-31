import itertools
import logging
import sys
import zmq
import ShoppingListCRDT
import json
import time
import sqlite3
import Item

REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 3
SERVER_ENDPOINT = "tcp://localhost:5555"

LRU_READY = "\x01"

class Client: 
    def __init__(self, port, username):
        self.username = username
        self.context = zmq.Context()
        self.brokerPorts = ["5555"]
        self.port = port 
        self.connected = True
        self.shopping_lists = []
        self.sls_offline = []
        self.items_offline = {}
        self.load_schema()
        self.run()
        

    def set_port(self, port):
        self.port = port    

    def get_port(self):
        return self.port

    
    def run(self): 
        while True: 
            print("1) Create new shopping list")
            print("2) Edit shopping list")
            print("3) Join shopping list")

            if self.connected:
                print("4) Go offline")
                
            else:
                print("4) Go online")
            
            print("5) Exit")

            
            choice = input("Enter your choice: ")

            if choice == "1":
                url = input("Enter the url of the shopping list: ")
                state = self.create_shopping_list(url)

                if state == -1: 
                    print("URL already in use")
                
            elif choice == "2":
                
                shopping_list = self.display_shopping_lists()
                

                if shopping_list == -1:
                    continue

                while True:
                    if self.connected:
                        sl = self.request_shopping_list(shopping_list.get_url())

                        if sl is not None:
                            shoppinglist = ShoppingListCRDT.ShoppingListCRDT(sl["url"], sl["additions"], sl["removals"])
                            shoppinglist.set_key(sl["key"])

                            shopping_list = shopping_list.merge(shoppinglist)

                    self.inspect_shopping_list(shopping_list)

                    print("1) Add item")
                    print("2) Remove item")
                    print("3) Exit")

                    choice2 = input("Enter your choice: ")
                    
                    if choice2 == "1":
                        shopping_list = self.add_item_shopping_list(shopping_list)
                    elif choice2 == "2": 
                        shopping_list = self.remove_item_shopping_list(shopping_list)
                    else:
                        break

                    for i in range(0, len(self.shopping_lists)): 
                        if self.shopping_lists[i].get_url() == shopping_list.get_url(): 
                            self.shopping_lists[i] = shopping_list
                            break

                    if self.connected:
                        self.send_shopping_list(shopping_list)

            elif choice == "3": 
                if self.connected: 
                    url = input("Enter the url of the shopping list: ")

                    key_exists = any(item.get_url() == url for item in self.shopping_lists)

                    if not key_exists: 
                        sl = self.request_shopping_list(url)
                        shopList = ShoppingListCRDT.ShoppingListCRDT(sl["url"], sl["additions"], sl["removals"])
                        shopList.set_key(sl["key"])
                        
                        self.shopping_lists.append(shopList)

                    else: 
                        print("You already have this shopping list")
                        continue
                else: 
                    print("You are not connected, cannot make requests to servers to join other shopping lists")

            elif choice == "4":
                if self.connected: 
                    self.connected = False
                    print("You are now offline")
                else:
                    self.connected = True
                    print("You are now online")
                    self.update_from_offline()

            elif choice == "5":
                print("exit")
                break
            else:
                print("Invalid choice")
        

    def display_shopping_lists(self):
        counter = 1
        dict = {}

        if self.shopping_lists == []:
            print("You have no shopping lists")
            return -1

        while True:
            for shopping_list in self.shopping_lists:
                dict[counter] = shopping_list
                print(str(counter) + ") " + shopping_list.get_url())
                counter += 1
            
            choice = input("Enter your choice: ")
            if int(choice) in dict:
                return dict[int(choice)]
            
            else:
                print("Invalid choice")
                counter = 1


    def add_item_shopping_list(self, shopping_list):
        item = input("Enter the item you want to add: ")
        quantity = input("Enter the quantity: ")

        for i in range(0, int(quantity)):
            shopping_list.add_item(item)

        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        cursor.execute("SELECT url FROM ShoppingLists WHERE url=?", (shopping_list.get_url(),))
        shopping_list_url = cursor.fetchone()[0]

        cursor.execute("UPDATE Items SET quantity = quantity + ? WHERE shopping_list_id = ? AND name = ?",
                    (int(quantity), shopping_list_url, item))

        if cursor.rowcount == 0:
            cursor.execute("INSERT INTO Items (shopping_list_id, name, quantity) VALUES (?, ?, ?)",
                        (shopping_list.url, item, int(quantity)))

        connection.commit()
        connection.close()

        return shopping_list
            

    def remove_item_shopping_list(self, shopping_list):
        item = input("Enter the item you want to remove: ")
        quantity = input("Enter the quantity: ")

        for i in range(0, int(quantity)):
            shopping_list.remove_item(item)

        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        if shopping_list.get_items()[item] != 0:
            cursor.execute("UPDATE Items SET quantity = ? WHERE shopping_list_id = ? AND name = ?",
                        (shopping_list.get_items()[item], shopping_list.get_url(), item))
        else:
            cursor.execute("DELETE FROM Items WHERE shopping_list_id = ? AND name = ?",
                        (shopping_list.get_url(), item))

        connection.commit()
        connection.close()

        return shopping_list

    def inspect_shopping_list(self, shoppinglist):
        shoppinglist.print_list()

    def pack_message(self, shoppinglist): 
        dictionary = {}

        dictionary["url"] = shoppinglist.get_url()
        dictionary["items"] = shoppinglist.get_items()
        dictionary["key"] = shoppinglist.get_key()
        dictionary["additions"] = shoppinglist.get_additions()
        dictionary["removals"] = shoppinglist.get_removals()

        return "sl:" + json.dumps(dictionary, sort_keys=True)

    def unpack_message(self, msg):
        return json.loads(msg[2])

    def update_from_offline(self): 

        for shopping_list in self.sls_offline: 
            logging.info("Connecting to server…")
            client = self.context.socket(zmq.DEALER)
            client.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
            client.connect("tcp://localhost:" + self.brokerPorts[0])
            client.send_multipart([b"sl", self.pack_message(shopping_list).encode('utf-8')])

            poller = zmq.Poller()
            poller.register(client, zmq.POLLIN)

            while True:
                events = dict(poller.poll())
                
                if client in events and events[client] == zmq.POLLIN:
                    reply = client.recv_multipart()
                    print(reply)

                    if "already exists" in reply[0].decode('utf-8'): 
                        self.shopping_lists.remove(shopping_list)

                        connection = sqlite3.connect(self.username + '.db')
                        cursor = connection.cursor()

                        cursor.execute("DELETE FROM ShoppingLists WHERE url = ?", (shopping_list.get_url(),))

                        connection.commit()
                        connection.close()
                    
                    else: 
                        connection = sqlite3.connect(self.username + '.db')
                        cursor = connection.cursor()

                        key_value = shopping_list.get_key() or ''
                        cursor.execute("SELECT COUNT(*) FROM ShoppingLists WHERE url = ?", (shopping_list.get_url(),))
                        result = cursor.fetchone()
                        if result[0] == 0:
                            cursor.execute("INSERT INTO ShoppingLists (client_username, url, key) VALUES (?, ?, ?)",
                                        (self.username, shopping_list.get_url(), key_value))

                        connection.commit()
                        connection.close()

                    break

            client.close()
    
    def create_shopping_list(self, url):

        shopping_list = ShoppingListCRDT.ShoppingListCRDT(url)

        if self.connected:
            self.shopping_lists.append(shopping_list)

            logging.info("Connecting to server…")
            client = self.context.socket(zmq.DEALER)
            client.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
            client.connect("tcp://localhost:" + self.brokerPorts[0])
            client.send_multipart([b"sl", self.pack_message(shopping_list).encode('utf-8')])

            poller = zmq.Poller()
            poller.register(client, zmq.POLLIN)

            while True:
                events = dict(poller.poll())
                
                if client in events and events[client] == zmq.POLLIN:
                    reply = client.recv_multipart()
                    print(reply)

                    if "already exists" in reply[0].decode('utf-8'): 
                        return -1
                    
                    else: 
                        connection = sqlite3.connect(self.username + '.db')
                        cursor = connection.cursor()

                        key_value = shopping_list.get_key() or ''
                        cursor.execute("SELECT COUNT(*) FROM ShoppingLists WHERE url = ?", (shopping_list.get_url(),))
                        result = cursor.fetchone()
                        if result[0] == 0:
                            cursor.execute("INSERT INTO ShoppingLists (client_username, url, key) VALUES (?, ?, ?)",
                                        (self.username, shopping_list.get_url(), key_value))

                        connection.commit()
                        connection.close()

                    break

        else: 

            connection = sqlite3.connect(self.username + '.db')
            cursor = connection.cursor()

            key_value = shopping_list.get_key() or ''
            cursor.execute("SELECT COUNT(*) FROM ShoppingLists WHERE url = ?", (shopping_list.get_url(),))
            result = cursor.fetchone()
            if result[0] == 0:
                cursor.execute("INSERT INTO ShoppingLists (client_username, url, key) VALUES (?, ?, ?)",
                            (self.username, shopping_list.get_url(), key_value))

            connection.commit()
            connection.close()
    
            self.sls_offline.append(shopping_list)
            self.shopping_lists.append(shopping_list)


    def request_shopping_list(self, url): 

        logging.info("Connecting to server…")
        client = self.context.socket(zmq.DEALER)
        client.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
        client.connect("tcp://localhost:" + self.brokerPorts[0])
        
        client.send_multipart([("requestSL:" + url).encode('utf-8')])

        poller = zmq.Poller()
        poller.register(client, zmq.POLLIN)

        sl = None

        while True:
            events = dict(poller.poll())
            
            if client in events and events[client] == zmq.POLLIN:
                reply = client.recv_multipart()
                reply = reply[0].decode('utf-8')

                if reply == "Shopping list not found": 
                    break
                
                sl = json.loads(reply)

                break

        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        cursor.execute("SELECT id FROM Clients WHERE username=?", (self.username,))

        cursor.execute("SELECT * FROM ShoppingLists WHERE client_username=? AND url=?", (self.username, url))
        existing_list = cursor.fetchone()

        if not existing_list:
            key_value = sl["key"] or ''
            cursor.execute("INSERT INTO ShoppingLists (client_username, url, key) VALUES (?, ?, ?)",
                        (self.username, url, str(key_value)))

        connection.commit()
        connection.close()

        return sl

    def send_shopping_list(self, shopping_list):

        logging.info("Connecting to server…")
        client = self.context.socket(zmq.DEALER)
        client.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
        client.connect("tcp://localhost:" + self.brokerPorts[0])

        client.send_multipart([b"sl", (self.pack_message(shopping_list)).encode('utf-8')])

        poller = zmq.Poller()
        poller.register(client, zmq.POLLIN)

        while True:
            events = dict(poller.poll())
            
            if client in events and events[client] == zmq.POLLIN:
                reply = client.recv_multipart()
                print(reply)
                break

        return
    

    def load_schema(self):
        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        with open('database/shopping_lists.sql', 'r') as sql_file:
            sql_script = sql_file.read()
            cursor.executescript(sql_script)
        
        cursor.execute("SELECT id FROM Clients WHERE username=? AND port=?", (self.username, self.port))
        existing_client = cursor.fetchone()

        if existing_client:
            print("Connecting to existing client...")

            cursor.execute("SELECT * FROM ShoppingLists WHERE client_username=?", (self.username,))
            sls = cursor.fetchall()

            for sl in sls:
                shopping_list = ShoppingListCRDT.ShoppingListCRDT(sl[2])
                self.shopping_lists.append(shopping_list)

        else:
            cursor.execute("INSERT INTO Clients (username, port) VALUES (?, ?)", (self.username, self.port))

        connection.commit()
        connection.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python ./Client.py <port> <username>")
        sys.exit(1)

    port = sys.argv[1]
    username = sys.argv[2]
    Client(port, username)