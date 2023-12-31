class ShoppingListCRDT:
    def __init__(self, url, additions=None, removals=None):
        if additions is None:
            additions = {}
        if removals is None:
            removals = {}

        self.additions = additions
        self.removals = removals
        self.url = url
        self.key = None

    def merge(self, remote):
        merged_additions = {item: max(self.additions.get(item, 0), remote.additions.get(item, 0)) for item in set(self.additions) | set(remote.additions)}
        merged_removals = {item: max(self.removals.get(item, 0), remote.removals.get(item, 0)) for item in set(self.removals) | set(remote.removals)}

        return ShoppingListCRDT(self.url, merged_additions, merged_removals)

    def add_item(self, item):
        self.additions[item] = self.additions.get(item, 0) + 1

    def remove_item(self, item):
        self.removals[item] = self.removals.get(item, 0) + 1

    def get_items(self):
        items = {}
        for item, count in self.additions.items():
            items[item] = count - self.removals.get(item, 0)
        return items
    
    def get_url(self):
        return self.url
    
    def get_key(self):
        return self.key
    
    def get_additions(self):
        return self.additions
    
    def get_removals(self):
        return self.removals
    
    def set_key(self, key): 
        self.key = key
    
    def print_list(self):
        print("Shopping list for ", self.url, ":")
        print(self.get_items())
