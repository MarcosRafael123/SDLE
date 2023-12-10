class Item: 
    def __init__(self, name, quantity):
        self.name = name
        self.quantity = quantity

    def add_quantity(self, quantity):
        self.quantity += quantity
    
    def remove_quantity(self, quantity):
        self.quantity -= quantity

    def get_name(self):
        return self.name
    
    def get_quantity(self):
        return self.quantity
