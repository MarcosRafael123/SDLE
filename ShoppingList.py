import sqlite3

class ShoppingList :
    def __init__(self, url, uuid):
        self.uuid = uuid
        self.url = url
        self.items = {}
        self.key = None
        self.timestamp = None

    # getters
    def get_uuid(self):
        return self.uuid

    def get_url(self):
        return self.url
    
    def get_items(self):
        return self.items
    
    def get_key(self):
        return self.key
    
    def get_timestamp(self):
        return self.timestamp
    
    # setters
    def set_timestamp(self, timestamp):
        self.timestamp = timestamp

    # other methods
    def print_list(self, client_username):
        print("Shopping list for " + self.url + ":"'\n')
         # Connect to the database and add the item to the Items table
        connection = sqlite3.connect(client_username + '.db')
        cursor = connection.cursor()

        # Retrieve the shopping list ID based on some identifier (e.g., URL)
        cursor.execute("SELECT id FROM ShoppingLists WHERE url=?", (self.url,))
        shopping_list_id = cursor.fetchone()[0]

        # Insert the item with the associated shopping list ID
        cursor.execute("SELECT * FROM Items WHERE shopping_list_id=?", (shopping_list_id,))
        self.items = cursor.fetchall()
        for item in self.items:
            print(str(item[2]) + ": " + str(item[3]))

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