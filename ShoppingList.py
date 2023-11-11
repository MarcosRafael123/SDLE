class ShoppingList :
    def __init__(self, url):
        self.url = url
        self.items = {}

    def add_item(self, item):
        self.items.append(item)

    def remove_item(self, item):
        self.items.remove(item)

    def get_url(self):
        return self.url
    
    def get_items(self):
        return self.items

    def print_list(self):
        print("Shopping list for " + self.name)
        for item in self.items:
            print(item)
