class Item: 
    def __init__(self, name):
        self.name = name
        self.quantity = 1

    def add_quantity(self, quantity):
        self.quantity += quantity
    
    def remove_quantity(self, quantity):
        self.quantity -= quantity

    def get_name(self):
        return self.name
    
    def get_quantity(self):
        return self.quantity
