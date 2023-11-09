import itertools
import logging
import sys
import zmq
import ShoppingList

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
        self.port = port 
        
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

        logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

        context = zmq.Context()

        logging.info("Connecting to server…")
        client = context.socket(zmq.REQ)
        client.connect("tcp://localhost:" + self.port)

        for sequence in itertools.count():
            request = str(sequence).encode()
            logging.info("Sending (%s)", request)
            client.send(request)

            retries_left = REQUEST_RETRIES
            while True:
                if (client.poll(REQUEST_TIMEOUT) & zmq.POLLIN) != 0:
                    reply = client.recv()
                    if int(reply) == sequence:
                        logging.info("Server replied OK (%s)", reply)
                        retries_left = REQUEST_RETRIES
                        break
                    else:
                        logging.error("Malformed reply from server: %s", reply)
                        continue

                retries_left -= 1
                logging.warning("No response from server")
                # Socket is confused. Close and remove it.
                client.setsockopt(zmq.LINGER, 0)
                client.close()
                if retries_left == 0:
                    logging.error("Server seems to be offline, abandoning")
                    sys.exit()

                logging.info("Reconnecting to server…")
                # Create new connection
                client = context.socket(zmq.REQ)
                client.connect("tcp://localhost:" + self.port)
                logging.info("Resending (%s)", request)
                client.send(request)

    def create_shopping_list(self, url):
        
        #shopping_list = ShoppingList.ShoppingList(url, items)

        # send to broker new shopping list
        context = zmq.Context()

        logging.info("Connecting to server…")
        client = context.socket(zmq.REQ)
        client.connect("tcp://localhost:" + self.port)
        client.send(url.encode('utf-8'))

        retries_left = REQUEST_RETRIES

        counter = 0

        while True:
            print(client.poll(REQUEST_TIMEOUT))
            print(zmq.POLLIN)
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
            client = context.socket(zmq.REQ)
            client.connect("tcp://localhost:" + self.port)
            logging.info("Resending (%s)", counter)
            client.send(url.encode('utf-8'))


def main():
    portFrontend = "5555"

    url = "http://my-first-shopping-list.com"
    items = None

    client = Client(portFrontend)

    client.create_shopping_list(url)

if __name__ == "__main__":
    main()