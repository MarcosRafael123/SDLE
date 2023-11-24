import itertools
import logging
import sys
import zmq
import ShoppingList
import json
import time

REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 3
SERVER_ENDPOINT = "tcp://localhost:5555"

LRU_READY = "\x01"

class Client: 
    def __init__(self, port): 
        """ self.username = username
        self.name = name
        self.email = email
        self.password = password
        self.host = None  """
        self.context = zmq.Context()
        self.brokerPorts = ["5555"]
        self.port = port 
        self.connected = True
        self.shopping_lists = []
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


    def add_item_shopping_list(self, shoppinglist):
        item = input("Enter the item you want to add: ")
        quantity = input("Enter the quantity: ")

        shoppinglist.add_item(item, int(quantity))
            

    def remove_item_shopping_list(self, shoppinglist):
        item = input("Enter the item you want to remove: ")
        quantity = input("Enter the quantity: ")

        shoppinglist.remove_item(item, int(quantity))

    def inspect_shopping_list(self, shoppinglist):
        shoppinglist.print_list()

    def pack_message(self, shoppinglist): 
        dictionary = {}

        dictionary["url"] = shoppinglist.get_url()
        dictionary["items"] = shoppinglist.get_items()
        dictionary["key"] = shoppinglist.get_key()
        dictionary["timestamp"] = shoppinglist.get_timestamp()

        return json.dumps(dictionary, sort_keys=True)

    def unpack_message(self, msg):
        return json.loads(msg[2])

    def create_shopping_list(self, url):
        
        shopping_list = ShoppingList.ShoppingList(url)
        shopping_list.set_timestamp(time.time())

        self.shopping_lists.append(shopping_list)

        logging.info("Connecting to server…")
        client = self.context.socket(zmq.REQ)
        client.connect("tcp://localhost:" + self.brokerPorts[0])
        client.send(self.pack_message(shopping_list).encode('utf-8'))

        retries_left = REQUEST_RETRIES

        counter = 0

        while True:
            print("CLIENT.POLL: ", client.poll(REQUEST_TIMEOUT))
            print("ZMQ.POLLIN: ", zmq.POLLIN)
            if (client.poll(REQUEST_TIMEOUT) & zmq.POLLIN) != 0:
                reply = client.recv_multipart()

                if reply[0].decode('utf-8') == "message received" : 
                    print(reply)
                    break
                else: 
                    print("error receiving")

            retries_left -= 1
            counter += 1
         
            # Socket is confused. Close and remove it.
            client.setsockopt(zmq.LINGER, 0)
            client.close()

            if retries_left == 0:
                logging.error("Server seems to be offline, abandoning")
                sys.exit()

            # Create new connection
            logging.info("Reconnecting to server…")
            client = self.context.socket(zmq.REQ)
            client.connect("tcp://localhost:" + self.port)
            logging.info("Resending (%s)", counter)
            client.send(url.encode('utf-8'))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python ./Client.py <port>")
        sys.exit(1)

    port = sys.argv[1]
    Client(port)