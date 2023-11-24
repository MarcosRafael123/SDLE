class ShoppingList :
    def __init__(self, url):
        self.url = url
        self.items = {}
        self.key = None

    def add_item(self, item, quantity):
        if item in self.items:
            self.items[item] += quantity
        else:
            self.items[item] = quantity

    def remove_item(self, item, quantity):
        if item in self.items:
            if quantity >= self.items[item]:
                del self.items[item]
            else:
                self.items[item] -= quantity

    def get_url(self):
        return self.url
    
    def get_items(self):
        return self.items
    
    def get_key(self):
        return self.key

    def print_list(self):
        print("Shopping list for " + self.url + ":")
        for item in self.items:
            print(item + ": " + str(self.items[item]))
