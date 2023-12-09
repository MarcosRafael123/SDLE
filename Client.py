import itertools
import logging
import sys
import zmq
import ShoppingList
import Item
import json
import time
import sqlite3
import uuid

REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 3
SERVER_ENDPOINT = "tcp://localhost:5555"

LRU_READY = "\x01"

class Client: 
    def __init__(self, port, username): 
        """ self.name = name
        self.email = email
        self.password = password
        self.host = None  """
        self.username = username
        self.context = zmq.Context()
        self.brokerPorts = ["5555"]
        self.port = port 
        self.connected = True
        self.shopping_lists = []
        self.load_schema()
        self.run()
        
    """ def set_host(self, host):
        self.host = host """

    def set_port(self, port):
        self.port = port    

    """ def get_username(self):
        return self.username

    def get_host(self):
        return self.host
     """
    def get_port(self):
        return self.port

    
    def run(self): 
        while True: 
            print("1) Create new shopping list")
            print("2) Edit shopping list")

            if self.connected:
                print("3) Go offline")
                
            else:
                print("3) Go online")
            
            print("4) Exit")

            
            choice = input("Enter your choice: ")

            if choice == "1":
                url = input("Enter the url of the shopping list: ")
                self.create_shopping_list(url)
                
            elif choice == "2":
                
                shopping_list = self.display_shopping_lists()

                if shopping_list == -1:
                    continue

                while True:
                    self.inspect_shopping_list(shopping_list)

                    print("\n1) Add item")
                    print("2) Remove item")
                    print("3) Exit")

                    choice2 = input("Enter your choice: ")
                    
                    if choice2 == "1":
                        self.add_item_shopping_list(shopping_list)
                    elif choice2 == "2": 
                        self.remove_item_shopping_list(shopping_list)
                    else:
                        break
                    
            elif choice == "3":
                if self.connected: 
                    self.connected = False
                    print("You are now offline")
                else:
                    self.connected = True
                    print("You are now online")

            elif choice == "4":
                print("exit")
                break
            else:
                print("Invalid choice")
        

    def display_shopping_lists(self):
        counter = 1
        dict = {}
        self.shopping_lists = []

        # Connect to the database and add the item to the Items table
        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        # Retrieve the shopping list ID based on some identifier (e.g., URL)
        cursor.execute("SELECT * FROM ShoppingLists WHERE client_username=?", (self.username,))
        sls = cursor.fetchall()

        for sl in sls:
            print(sl)
            shopping_list = ShoppingList.ShoppingList(sl[2], sl[0])
            shopping_list.set_timestamp(sl[4])
            self.shopping_lists.append(shopping_list)


        connection.commit()
        connection.close()

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

        #item = Item.Item(name, quantity)

        shopping_list.add_item(item, int(quantity))

            
        # Connect to the database and add the item to the Items table
        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        # Retrieve the shopping list ID based on some identifier (e.g., URL)
        cursor.execute("SELECT id FROM ShoppingLists WHERE id=?", (shopping_list.get_uuid(),))
        shopping_list_id = cursor.fetchone()[0]

        # Try to insert the item; if it already exists, it will be ignored
        # Try to update the quantity of an existing item
        cursor.execute("UPDATE Items SET quantity = quantity + ? WHERE shopping_list_id = ? AND name = ?",
                    (int(quantity), shopping_list_id, item))

        # Check if the update affected any rows; if not, insert a new item
        if cursor.rowcount == 0:
            cursor.execute("INSERT INTO Items (shopping_list_id, name, quantity) VALUES (?, ?, ?)",
                        (shopping_list_id, item, int(quantity)))
        
        print(time.time())
        shopping_list.set_timestamp(time.time())

        print(shopping_list.get_uuid())
        print(item)
        cursor.execute("SELECT quantity FROM Items WHERE shopping_list_id=? AND name=?", (shopping_list.get_uuid(),item))
        item_quantity = cursor.fetchone()[0]
        print("ITRBAWNDNAWDUA: ", item_quantity)

        connection.commit()
        connection.close()

        logging.info("Connecting to server…")
        client = self.context.socket(zmq.DEALER)
        client.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
        client.connect("tcp://localhost:" + self.brokerPorts[0])
        print("ASDFGHJKL: ", self.pack_message(shopping_list).encode('utf-8'))
        shopping_list.set_item_quantity(item, item_quantity)
        print("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
        client.send_multipart([b"sl", self.pack_message(shopping_list).encode('utf-8')])

        """ poller = zmq.Poller()
        poller.register(client, zmq.POLLIN)

        while True:
            events = dict(poller.poll())
            
            if client in events and events[client] == zmq.POLLIN:
                reply = client.recv_multipart()
                print(reply)
                break """

    def remove_item_shopping_list(self, shopping_list):
        item = input("Enter the item you want to remove: ")
        quantity = input("Enter the quantity: ")

        #item = Item.Item(name, quantity)

        shopping_list.remove_item(item, int(quantity))

        # Connect to the database and add the item to the Items table
        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        # Retrieve the shopping list ID based on some identifier (e.g., URL)
        cursor.execute("SELECT id FROM ShoppingLists WHERE id=?", (shopping_list.get_uuid(),))
        shopping_list_id = cursor.fetchone()[0]
        
        cursor.execute("UPDATE Items SET quantity = CASE WHEN (quantity - ?) < 0 THEN 0 ELSE (quantity - ?) END WHERE shopping_list_id = ? AND name = ?",
                       (int(quantity), int(quantity), shopping_list_id, item)
)
        
        print(time.time())
        shopping_list.set_timestamp(time.time())

        print(shopping_list.get_uuid())
        print(item)
        cursor.execute("SELECT quantity FROM Items WHERE shopping_list_id=? AND name=?", (shopping_list.get_uuid(),item))
        item_quantity = cursor.fetchone()[0]
        print("AFDOADOADOA;DAOD;AWOD;AWD;AOD;AWOD;AWOD;ADO;A;DOAW;DOAW;:", item_quantity)
        """ if cursor.fetchall() == []:
            item_quantity = 0
        else:
            print("ITRBAWNDNAWDUA: ", item_quantity) """
        
        if item_quantity == 0:
            cursor.execute("DELETE FROM Items WHERE shopping_list_id = ? AND name = ?",
               (shopping_list_id, item))
        

        connection.commit()
        connection.close()

        logging.info("Connecting to server…")
        client = self.context.socket(zmq.DEALER)
        client.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
        client.connect("tcp://localhost:" + self.brokerPorts[0])
        shopping_list.set_item_quantity(item, item_quantity)
        client.send_multipart([b"sl", self.pack_message(shopping_list).encode('utf-8')])

        """ poller = zmq.Poller()
        poller.register(client, zmq.POLLIN)

        while True:
            events = dict(poller.poll())
            
            if client in events and events[client] == zmq.POLLIN:
                reply = client.recv_multipart()
                print(reply)
                break """


    def inspect_shopping_list(self, shoppinglist):
        shoppinglist.print_list(self.username)

    def pack_message(self, shoppinglist): 
        print("SHOPPING LIST ITEMS: ", shoppinglist.get_items())
        if shoppinglist.get_items() == {}:

            dictionary = {}

            dictionary["uuid"] = shoppinglist.get_uuid()
            dictionary["url"] = shoppinglist.get_url()
            dictionary["items"] = shoppinglist.get_items()
            dictionary["key"] = shoppinglist.get_key()
            dictionary["timestamp"] = shoppinglist.get_timestamp()

            return "sl:" + json.dumps(dictionary, sort_keys=True)
        else:
            dictionary = {}

            dictionary["uuid"] = shoppinglist.get_uuid()
            dictionary["url"] = shoppinglist.get_url()
            dictionary["items"] = shoppinglist.get_items()
            dictionary["key"] = shoppinglist.get_key()
            dictionary["timestamp"] = shoppinglist.get_timestamp()

            print("miofnsefninvfnisniniosnioesnbiosniobfes")
            return "sl:" + json.dumps(dictionary, sort_keys=True)

    def unpack_message(self, msg):
        return json.loads(msg[2])

    def create_shopping_list(self, url):

        new_uuid = str(uuid.uuid4())
        shopping_list = ShoppingList.ShoppingList(url, new_uuid)
        shopping_list.set_timestamp(time.time())

        self.shopping_lists.append(shopping_list)

        # Insert the shopping list into the database with the corresponding client ID
        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        # Retrieve the client ID based on the username
        cursor.execute("SELECT id FROM Clients WHERE username=?", (self.username,))

        # Insert the shopping list with the associated client ID
        key_value = shopping_list.get_key() or ''
        cursor.execute("INSERT INTO ShoppingLists (id, client_username, url, key, timestamp) VALUES (?, ?, ?, ?, ?)",
                       (new_uuid, self.username, url, key_value, shopping_list.get_timestamp()))

        connection.commit()
        connection.close()

        logging.info("Connecting to server…")
        client = self.context.socket(zmq.DEALER)
        client.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
        client.connect("tcp://localhost:" + self.brokerPorts[0])
        client.send_multipart([b"sl", self.pack_message(shopping_list).encode('utf-8')])

        """ poller = zmq.Poller()
        poller.register(client, zmq.POLLIN)

        while True:
            events = dict(poller.poll())
            
            if client in events and events[client] == zmq.POLLIN:
                reply = client.recv_multipart()
                print(reply)
                break """

    def load_schema(self):
        # Connect to the SQLite database
        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        # Read and execute the SQL file
        with open('database/shopping_lists.sql', 'r') as sql_file:
            print("EWEWREWFWEFEFREGH")
            sql_script = sql_file.read()
            cursor.executescript(sql_script)
        
        # Check if the client already exists
        cursor.execute("SELECT id FROM Clients WHERE username=? AND port=?", (self.username, self.port))
        existing_client = cursor.fetchone()

        if existing_client:
            print("Connecting to existing client...")
            # Handle connecting to the existing client here
            # Maybe set some instance variables to keep track of this client
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