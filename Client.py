class Client: 
    def __init__(self, username, name, email, password): 
        self.username = username
        self.name = name
        self.email = email
        self.password = password
        self.host = None 
        self.port = None 
        
    def set_host(self, host):
        self.host = host

    def set_port(self, port):
        self.port = port    

    def get_username(self):
        return self.username

    def get_host(self):
        return self.host
    
    def get_port(self):
        return self.port
    