import itertools
import logging
import sys
import zmq
import ShoppingListCRDT
import json
import time
import sqlite3

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
            print("3) Join shopping list")

            if self.connected:
                print("4) Go offline")
                
            else:
                print("4) Go online")
            
            print("5) Exit")

            
            choice = input("Enter your choice: ")

            if choice == "1":
                url = input("Enter the url of the shopping list: ")
                self.create_shopping_list(url)
                
            elif choice == "2":
                
                shopping_list = self.display_shopping_lists()

                if shopping_list == -1:
                    continue

                while True:
                    sl = self.request_shopping_list(shopping_list.get_url())

                    print(sl)

                    if sl is not None:
                        shoppinglist = ShoppingListCRDT.ShoppingListCRDT(sl["url"], sl["additions"], sl["removals"])
                        shoppinglist.set_key(sl["key"])

                        print("Before merge: ", shopping_list.get_items())

                        shopping_list = shopping_list.merge(shoppinglist)

                        print("After merge: ", shopping_list.get_items())

                        # Connect to the database and add the item to the Items table
                        connection = sqlite3.connect(self.username + '.db')
                        cursor = connection.cursor()

                        # Retrieve the shopping list ID based on some identifier (e.g., URL)
                        """ cursor.execute("SELECT id FROM ShoppingLists WHERE url=?", (shopping_list.get_url(),))
                        shopping_list_url = cursor.fetchone()[0] """

                        # Try to insert the item; if it already exists, it will be ignored
                        # Try to update the quantity of an existing item
                        print("SHOPPING LIST GET ITEMS: ", shopping_list.get_items())
                        for key, value in shopping_list.get_items().items():
                            print("KEY:", key)
                            print("VALUE: ", value)
                            cursor.execute("UPDATE Items SET quantity = ? WHERE shopping_list_id = ? AND name = ?",
                                        (value, shopping_list.get_url(), key))

                            # Check if the update affected any rows; if not, insert a new item
                            if cursor.rowcount == 0:
                                cursor.execute("INSERT INTO Items (shopping_list_id, name, quantity) VALUES (?, ?, ?)",
                                            (shopping_list.get_url(), key, value))

                        connection.commit()
                        connection.close()

                    self.inspect_shopping_list(shopping_list)

                    print("1) Add item")
                    print("2) Remove item")
                    print("3) Exit")

                    choice2 = input("Enter your choice: ")
                    
                    if choice2 == "1":
                        self.add_item_shopping_list(shopping_list)
                    elif choice2 == "2": 
                        self.remove_item_shopping_list(shopping_list)
                    else:
                        break
                  
                    print("After editing: ", shopping_list.get_items())

                    self.send_shopping_list(shopping_list)

            elif choice == "3": 
                url = input("Enter the url of the shopping list: ")

                key_exists = any(item.get_url() == url for item in self.shopping_lists)

                if not key_exists: 
                    sl = self.request_shopping_list(url)
                    print(sl)
                    shopList = ShoppingListCRDT.ShoppingListCRDT(sl["url"], sl["additions"], sl["removals"])
                    shopList.set_key(sl["key"])
                    
                    self.shopping_lists.append(shopList)

                else: 
                    print("You already have this shopping list")
                    continue

            elif choice == "4":
                if self.connected: 
                    self.connected = False
                    print("You are now offline")
                else:
                    self.connected = True
                    print("You are now online")

            elif choice == "5":
                print("exit")
                break
            else:
                print("Invalid choice")
        

    def display_shopping_lists(self):
        counter = 1
        dict = {}
        shopping_lists = []

        # Connect to the database and add the item to the Items table
        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        # Retrieve the shopping list ID based on some identifier (e.g., URL)
        cursor.execute("SELECT * FROM ShoppingLists WHERE client_username=?", (self.username,))
        sls = cursor.fetchall()

        for sl in sls:
            print(sl)
            shopping_list = ShoppingListCRDT.ShoppingListCRDT(sl[2])
            shopping_lists.append(shopping_list)


        connection.commit()
        connection.close()

        if shopping_lists == []:
            print("You have no shopping lists")
            return -1

        while True:
            for shopping_list in shopping_lists:
                print(shopping_list.get_url())
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

        # Connect to the database and add the item to the Items table
        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        # Retrieve the shopping list ID based on some identifier (e.g., URL)
        """ cursor.execute("SELECT id FROM ShoppingLists WHERE url=?", (shopping_list.get_url(),))
        shopping_list_url = cursor.fetchone()[0] """

        # Try to insert the item; if it already exists, it will be ignored
        # Try to update the quantity of an existing item
        cursor.execute("UPDATE Items SET quantity = ? WHERE shopping_list_id = ? AND name = ?",
                    (shopping_list.get_items()[item], shopping_list.get_url(), item))

        # Check if the update affected any rows; if not, insert a new item
        if cursor.rowcount == 0:
            cursor.execute("INSERT INTO Items (shopping_list_id, name, quantity) VALUES (?, ?, ?)",
                        (shopping_list.get_url(), item, shopping_list.get_items()[item]))

        connection.commit()
        connection.close()
            

    def remove_item_shopping_list(self, shopping_list):
        item = input("Enter the item you want to remove: ")
        quantity = input("Enter the quantity: ")

        for i in range(0, int(quantity)):
            shopping_list.remove_item(item)

        # Connect to the database and add the item to the Items table
        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        # Retrieve the shopping list ID based on some identifier (e.g., URL)
        """ cursor.execute("SELECT id FROM ShoppingLists WHERE url=?", (shopping_list.get_url(),))
        shopping_list_url = cursor.fetchone()[0] """

        """ cursor.execute("UPDATE Items SET quantity = CASE WHEN (quantity - ?) < 0 THEN 0 ELSE (quantity - ?) END WHERE shopping_list_id = ? AND name = ?",
                       (int(quantity), int(quantity), shopping_list_url, item)) """

        if shopping_list.get_items()[item] != 0:
            cursor.execute("UPDATE Items SET quantity = ? WHERE shopping_list_id = ? AND name = ?",
                        (shopping_list.get_items()[item], shopping_list.get_url(), item))
        else:
            cursor.execute("DELETE FROM Items WHERE shopping_list_id = ? AND name = ?",
                        (shopping_list.get_url(), item))

        connection.commit()
        connection.close()

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

    def create_shopping_list(self, url):
        
        shopping_list = ShoppingListCRDT.ShoppingListCRDT(url)
        #shopping_list.set_timestamp(time.time())

        self.shopping_lists.append(shopping_list)

        # Insert the shopping list into the database with the corresponding client ID
        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        # Retrieve the client ID based on the username
        cursor.execute("SELECT id FROM Clients WHERE username=?", (self.username,))

        # Insert the shopping list with the associated client ID
        key_value = shopping_list.get_key() or ''
        cursor.execute("SELECT COUNT(*) FROM ShoppingLists WHERE url = ?", (url,))
        result = cursor.fetchone()
        if result[0] == 0:
            cursor.execute("INSERT INTO ShoppingLists (client_username, url, key) VALUES (?, ?, ?)",
                        (self.username, url, key_value))

        connection.commit()
        connection.close()

        if self.connected:  # Check the number of elements in the list
            logging.info("Connecting to server…")
            client = self.context.socket(zmq.DEALER)
            client.setsockopt_string(zmq.IDENTITY, str(self.port), 'utf-8')
            client.connect("tcp://localhost:" + self.brokerPorts[0])
            for sl in self.shopping_lists:
                print("Shopping lists remaining: ", len(self.shopping_lists))
                print("Sending message for:", sl.get_url())
                client.send_multipart([b"sl", self.pack_message(sl).encode('utf-8')])
                print("Message sent")

                poller = zmq.Poller()
                poller.register(client, zmq.POLLIN)

                while True:
                    events = dict(poller.poll())
                    
                    if client in events and events[client] == zmq.POLLIN:
                        reply = client.recv_multipart()
                        print(reply)
                        #print("Erasing sl with url:", sl.get_url())
                        #self.shopping_lists.pop(0)
                        break
            self.shopping_lists.clear()
            
            
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
                print(reply)
                reply = reply[0].decode('utf-8')

                if reply == "Shopping list not found": 
                    break
                
                sl = json.loads(reply)

                print("SL AFTER LOADS: ", sl)
                break

        # Insert the shopping list into the database with the corresponding client ID
        connection = sqlite3.connect(self.username + '.db')
        cursor = connection.cursor()

        # Retrieve the client ID based on the username
        cursor.execute("SELECT id FROM Clients WHERE username=?", (self.username,))

        cursor.execute("SELECT * FROM ShoppingLists WHERE client_username=? AND url=?", (self.username, url))
        existing_list = cursor.fetchone()

        if not existing_list:
            # Insert the shopping list with the associated client ID
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