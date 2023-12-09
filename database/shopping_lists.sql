CREATE TABLE IF NOT EXISTS Clients (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL UNIQUE, -- Ensuring username is unique per client
    port INTEGER NOT NULL UNIQUE -- Ensuring the port is unique per client
);

CREATE TABLE IF NOT EXISTS ShoppingLists (
    id TEXT PRIMARY KEY,
    client_username INTEGER NOT NULL,
    url TEXT NOT NULL UNIQUE,
    key TEXT,
    timestamp INTEGER NOT NULL,
    FOREIGN KEY(client_username) REFERENCES Clients(username)
);

CREATE TABLE IF NOT EXISTS Items (
    id INTEGER PRIMARY KEY,
    shopping_list_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    FOREIGN KEY(shopping_list_id) REFERENCES ShoppingLists(id)
);

CREATE TABLE IF NOT EXISTS ShoppingListsServers (
    id TEXT PRIMARY KEY,
    server_port INTEGER NOT NULL,
    key TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    timestamp INTEGER NOT NULL,
    FOREIGN KEY(server_port) REFERENCES Servers(port)
);

CREATE TABLE IF NOT EXISTS ItemsServers (
    id INTEGER PRIMARY KEY,
    shopping_list_servers_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    FOREIGN KEY(shopping_list_servers_id) REFERENCES ShoppingListsServers(id)
);

CREATE TABLE IF NOT EXISTS Servers (
    id INTEGER PRIMARY KEY,
    key TEXT NOT NULL,
    port INTEGER NOT NULL UNIQUE
);