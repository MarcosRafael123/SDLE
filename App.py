import Broker
import Client
import Server

def main():
    portFrontend = "5555"
    portBackend = "5556"

    broker = Broker.Broker(portFrontend, portBackend)
    server = Server.Server(portBackend)
    client = Client.Client(portFrontend)

    broker.run()
    server.run()
    client.run()

if __name__ == "__main__":
    main()

