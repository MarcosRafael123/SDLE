class ShoppingList :
    def __init__(self, name, items):
        self.name = name
        self.items = items

    def add_item(self, item):
        self.items.append(item)

    def remove_item(self, item):
        self.items.remove(item)

    def get_name(self):
        return self.name
    
    def get_items(self):
        return self.items

    def print_list(self):
        print("Shopping list for " + self.name)
        for item in self.items:
            print(item)
