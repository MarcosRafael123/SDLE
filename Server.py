from random import randint
import time
import zmq

LRU_READY = "\x01"

class Server: 
    def __init__(self, port): 
        self.port = port 
        self.shopping_lists = []

    def set_port(self, port):
        self.port = port

    def get_port(self):
        return self.port
    
    def run(self):

        context = zmq.Context(1)
        worker = context.socket(zmq.REQ)

        identity = "%04X-%04X" % (randint(0, 0x10000), randint(0,0x10000))
        worker.setsockopt_string(zmq.IDENTITY, identity)
        worker.connect("tcp://localhost:" + self.port)

        print("I: (%s) worker ready" % identity)
        worker.send_string(LRU_READY)

        cycles = 0
        while True:
            msg = worker.recv_multipart()
            if not msg:
                break

            cycles += 1
            if cycles>0 and randint(0, 5) == 0:
                print("I: (%s) simulating a crash" % identity)
                break
            elif cycles>3 and randint(0, 5) == 0:
                print("I: (%s) simulating CPU overload" % identity)
                time.sleep(3)
            print("I: (%s) normal reply" % identity)
            time.sleep(1) # Do some heavy work
            worker.send_multipart(msg)

def main():
    portFrontend = "5555"
    portBackend = "5556"

    server = Server(portBackend)

    server.run()

if __name__ == "__main__":
    main()