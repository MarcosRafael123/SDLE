-- Insert shopping lists
INSERT INTO shopping_list (url, name) VALUES ('http://my-first-shopping-list.com', 'My first shopping list');
INSERT INTO shopping_list (url, name) VALUES ('http://my-second-shopping-list.com', 'My second shopping list');
INSERT INTO shopping_list (url, name) VALUES ('http://my-third-shopping-list.com', 'My third shopping list');

-- Insert shopping list items
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Milk', 1, 1);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Eggs', 12, 1);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Bread', 2, 1);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Butter', 1, 1);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Cheese', 1, 1);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Ham', 1, 1);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Bacon', 1, 1);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Orange juice', 1, 1);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Apple juice', 1, 2);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Pineapple juice', 1, 2);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Peach juice', 1, 2);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Strawberry juice', 1, 2);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Banana juice', 1, 2);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Mango juice', 1, 3);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Grape juice', 1, 3);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Watermelon juice', 1, 3);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Kiwi juice', 1, 3);
INSERT INTO shopping_list_item (name, quantity, shopping_list_id) VALUES ('Lemon juice', 1, 3);

-- Insert clients
INSERT INTO client (username, name, email, password) VALUES ('johndoe', 'John Doe', 'johndoe@email.com', 'password');
INSERT INTO client (username, name, email, password) VALUES ('janedoe', 'Jane Doe', 'janedoe@email.com', 'password');
INSERT INTO client (username, name, email, password) VALUES ('johnsmith', 'John Smith', 'johnsmith@email.com', 'password');
INSERT INTO client (username, name, email, password) VALUES ('janesmith', 'Jane Smith', 'janesmith@email.com', 'password');
INSERT INTO client (username, name, email, password) VALUES ('javiusmith', 'Javiu Smith', 'javiusmith@email.com', 'password');
INSERT INTO client (username, name, email, password) VALUES ('kindanewe', 'Kinda Newe', 'kindanewe@email.com', 'password');
INSERT INTO client (username, name, email, password) VALUES ('newekinda', 'Newe Kinda', 'newekinda@email.com', 'password');
INSERT INTO client (username, name, email, password) VALUES ('magcosrafi', 'Magcos Rafi', 'magcosrafi@email.com', 'password');
INSERT INTO client (username, name, email, password) VALUES ('josephlouis', 'Joseph Louis', 'josephlouis@email.com', 'password');