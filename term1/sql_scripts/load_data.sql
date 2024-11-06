-- Data Engineering 1: Term Project 1
-- Tatyana Yakushina
-- Script 1 out of 3: OPERATIONAL LAYER
-- This script imports 4 datasets to create a database for pizza sales.
-- -----------------------------------------------------------------------

DROP SCHEMA IF EXISTS pizza_db;
CREATE SCHEMA pizza_db;
USE pizza_db; 

-- Setting up
SHOW VARIABLES LIKE "secure_file_priv";
SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 'ON';
SHOW GLOBAL VARIABLES LIKE 'local_infile';

-- Dataset 1: 'pizzas' 
DROP TABLE IF EXISTS pizzas;
CREATE TABLE pizzas
(`pizza_id` varchar(50) NOT NULL, 
`pizza_type_id` VARCHAR(50), 
`size` VARCHAR(20), 
`price` double,
PRIMARY KEY(pizza_id));

LOAD DATA LOCAL INFILE 'D:/save/Edu/CEU/Data eng 1/term project 1/Pizza+Place+Sales/pizza_sales/pizzas.csv' 
INTO TABLE pizzas
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES 
(pizza_id, pizza_type_id, size, price);

-- Dataset 2: 'pizza_types'
DROP TABLE IF EXISTS pizza_types;
CREATE TABLE pizza_types (
    pizza_type_id varchar(50) NOT NULL,
    name VARCHAR(100),
    category VARCHAR(50),
    ingredients text,
	PRIMARY KEY(pizza_type_id)
);

-- Important to character set to latin1 for compatibility
LOAD DATA LOCAL INFILE 'D:/save/Edu/CEU/Data eng 1/term project 1/Pizza+Place+Sales/pizza_sales/pizza_types.csv'
INTO TABLE pizza_types
CHARACTER SET latin1 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

-- Dataset 3: 'orders'
DROP TABLE IF EXISTS orders;
CREATE TABLE `pizza_db`.`orders` 
(`order_id` int NOT NULL, 
`date` date, 
`time` text, 
PRIMARY KEY(order_id));

LOAD DATA LOCAL INFILE 'D:/save/Edu/CEU/Data eng 1/term project 1/Pizza+Place+Sales/pizza_sales/orders.csv' 
INTO TABLE orders
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES 
(order_id, date, time);

-- Convert the time field to a proper datetime format
UPDATE `pizza_db`.`orders`
SET `time` = STR_TO_DATE(`time`,'%H:%i:%S'); # ! CHECK

-- Dataset 4: 'order_details'
DROP TABLE IF EXISTS order_details;
CREATE TABLE `pizza_db`.`order_details` 
(`order_details_id` int NOT NULL, 
`order_id` int, 
`pizza_id` text, 
`quantity` int,
PRIMARY KEY(order_details_id));

LOAD DATA LOCAL INFILE 'D:/save/Edu/CEU/Data eng 1/term project 1/Pizza+Place+Sales/pizza_sales/order_details.csv' 
INTO TABLE order_details
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES 
(order_details_id, order_id, pizza_id, quantity);


-- Create a denormalized table
DROP TABLE IF EXISTS pizza_order_analysis;
CREATE TABLE pizza_order_analysis AS
SELECT orders.order_id, date, time, order_details_id, 
    quantity, order_details.pizza_id, pizzas.pizza_type_id, 
    size, price, name, category, ingredients, 
    CASE 
        WHEN ingredients LIKE '%Olives%' THEN 1 
        ELSE 0 
    END AS has_olives, 
    CASE 
        WHEN MONTH(date) IN (12, 1, 2) THEN 'winter' 
        WHEN MONTH(date) IN (3, 4, 5) THEN 'spring'
        WHEN MONTH(date) IN (6, 7, 8) THEN 'summer' 
        ELSE 'autumn' 
    END AS season,
	CASE 
        WHEN time BETWEEN '10:00:00' AND '23:00:00' THEN 1 
        ELSE 0 
    END AS working_hours
FROM orders
INNER JOIN 
    order_details ON orders.order_id = order_details.order_id
LEFT JOIN 
    pizzas ON pizzas.pizza_id = order_details.pizza_id
LEFT JOIN 
    pizza_types ON pizza_types.pizza_type_id = pizzas.pizza_type_id;
