DROP TABLE IF EXISTS shopping_list; 
DROP TABLE IF EXISTS shopping_list_item;
DROP TABLE IF EXISTS client;

CREATE TABLE IF NOT EXISTS shopping_list (
    url TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shopping_list_item (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    quantity INTEGER,
    shopping_list_id INTEGER NOT NULL, 
    FOREIGN KEY (shopping_list_id) REFERENCES shopping_list (url)
);

CREATE TABLE IF NOT EXISTS client (
    username TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    password TEXT NOT NULL
);