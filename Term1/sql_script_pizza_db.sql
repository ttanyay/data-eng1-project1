-- Data Engineering 1: Term Project 1
-- Tatyana Yakushina

-- -----------------------------------------------------------------------
-- OPERATIONAL LAYER
-- This part of the script imports 4 datasets to create a database for pizza sales.
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

LOAD DATA LOCAL INFILE '../input_data/pizzas.csv' 
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
LOAD DATA LOCAL INFILE '../input_data/pizza_types.csv'
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

LOAD DATA LOCAL INFILE '../input_data/orders.csv' 
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

LOAD DATA LOCAL INFILE '../input_data/order_details.csv' 
INTO TABLE order_details
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES 
(order_details_id, order_id, pizza_id, quantity);
-- -----------------------------------------------------------------------
-- ANALYTICS
-- -----------------------------------------------------------------------
-- Plan of executed analytics:
-- 1. Olive Preference Hypothesis
-- Hypothesis: Customers prefer pizzas without olives
-- Approach: Compare the total quantity and percentages of pizzas ordered with 
-- and without olives as an ingredient (by months). 

-- 2. When is the restaurant busiest?
-- Hypothesis: lunch and dinner time are the busiest. 
-- Approach: Aggregate the number of orders during lunch (12:00 PM - 2:00 PM) 
-- and dinner (6:00 PM - 9:00 PM) hours and compare them with other hours of the day. 
-- Within the ETL, include a trigger that reacts if an order is made outside 
-- working hours (described more in the script etl_and_data_mart). 

-- 3. Analyze Seasonal Popularity of the Top Five Pizzas
-- Hypothesis: The top five most popular pizzas have higher sales in specific 
-- seasons, indicating a seasonal preference among customers.
-- Approach: Using data mart views, compare the order quantities of the top 
-- three pizzas across different seasons to identify any significant seasonal 
-- trends in their popularity.

-- 4. Order Quantity and Price Hypothesis:
-- Hypothesis: Lower-priced pizzas are ordered in higher quantities.
-- Approach: Perform a correlation analysis between pizza price and order 
-- quantity. A negative correlation would support this hypothesis.

-- Comment: the hypothesis testing was aimed to be as efficient as possible 
-- within the scope of data.


-- -----------------------------------------------------------------------
-- ANALYTICAL LAYER
-- This part of the script creates a denormalized table 'pizza_order_analysis' 
-- for analysis purposes.
-- -----------------------------------------------------------------------


DROP TABLE IF EXISTS pizza_order_analysis;
CREATE TABLE pizza_order_analysis AS
SELECT orders.order_id, date, time, order_details_id, 
    quantity, order_details.pizza_id, pizzas.pizza_type_id, 
    size, price, name, category, ingredients, 
    -- Assign season based on the order's month
    CASE 
        WHEN MONTH(date) IN (12, 1, 2) THEN 'winter' 
        WHEN MONTH(date) IN (3, 4, 5) THEN 'spring'
        WHEN MONTH(date) IN (6, 7, 8) THEN 'summer' 
        ELSE 'autumn' 
    END AS season,
    -- Check if the order is during working hours (10 AM to 11 PM)
    CASE 
        WHEN time BETWEEN '10:00:00' AND '23:00:00' THEN 1 
        ELSE 0 
    END AS working_hours,
	-- Flag for pizzas with olives
    CASE 
        WHEN ingredients LIKE '%Olives%' THEN 1 
        ELSE 0 
    END AS has_olives
FROM orders
-- join orders and order_details (left join also works here)
INNER JOIN 
    order_details ON orders.order_id = order_details.order_id
-- include pizza details
LEFT JOIN 
    pizzas ON pizzas.pizza_id = order_details.pizza_id
-- include pizza type details
LEFT JOIN 
    pizza_types ON pizza_types.pizza_type_id = pizzas.pizza_type_id;


-- -----------------------------------------------------------------------
-- ETL PIPLINE: 
-- This part of the script creates an ETL pipeline including Triggers and Stored procedures.
-- -----------------------------------------------------------------------
-- Below two procedures and two triggers are created and tested. 

-- A stored procedure is defined to add new order details, automatically 
-- assigning unique IDs and inserting associated entries into the orders 
-- and order_details tables. One trigger is defined to verify existence of 
-- order_id and pizza_id before insertions, ensuring data integrity.

-- Procedure to store the number of orders (needed later for the trigger)
DROP PROCEDURE IF EXISTS add_order_detail;

DELIMITER //
CREATE PROCEDURE add_order_detail(
	IN pizza_id VARCHAR(50),
IN quantity INT)
BEGIN
	DECLARE new_order_id INT;DECLARE new_order_details_id INT;
	-- calculate the next order_id
	SET new_order_id = (SELECT IFNULL(MAX(order_id), 0) + 1 FROM orders);
-- calculate the next order_details_id
SET new_order_details_id = (SELECT IFNULL(MAX(order_details_id), 0) + 1 FROM order_details);
-- insert a new order entry if it does not exist
	INSERT INTO orders (order_id, date, time)VALUES (new_order_id, CURDATE(), CURTIME());
	-- insert into order_details with the calculated new_order_id and new_order_details_id
	INSERT INTO order_details (order_details_id, order_id, pizza_id, quantity)
	VALUES (new_order_details_id, new_order_id, pizza_id, quantity);
END; //
DELIMITER;
-- Extract is SELECT, Transformation is adding +1 and Loading is INSERT INTO

-- Trigger: 
DROP TRIGGER IF EXISTS insert_order_and_detail;

DELIMITER //

CREATE TRIGGER insert_order_and_detail
BEFORE INSERT ON order_details
FOR EACH ROW
BEGIN
    DECLARE orderExists INT;
    DECLARE pizzaExists INT;
    
    -- Check if the order exists in the orders table
    SET orderExists = (SELECT COUNT(*) FROM orders WHERE order_id = NEW.order_id);
    
    -- If the order doesn't exist, insert a new order with the current date and time
    IF orderExists = 0 THEN
        INSERT INTO orders (order_id, date, time)
        VALUES (NEW.order_id, CURDATE(), CURTIME());
    END IF;
    
    -- Check if the pizza exists in the pizzas table
    SET pizzaExists = (SELECT COUNT(*) FROM pizzas WHERE pizza_id = NEW.pizza_id);
    
    -- If the pizza ID does not exist, throw an error
    IF pizzaExists = 0 THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'Pizza ID does not exist in pizzas table';
    END IF;
END //

DELIMITER ;

-- Test 1
-- Successfully passed the test
CALL add_order_detail('big_meat_s', 2);

-- Test 2
-- This fails, because there is no such pizza as 'tanya' :(
-- CALL add_order_detail('tanya', 10);

-- The procedure is not very efficient, because MySql has auto increment option, 
-- but this procedure was only for an example.

-- Create a procedure to do more complicated tests.
DROP PROCEDURE IF EXISTS generate_random_orders;
DELIMITER //

CREATE PROCEDURE generate_random_orders(IN num_orders INT)
BEGIN
    DECLARE i INT DEFAULT 0;
    
    WHILE i < num_orders DO
        SET @random_pizza = (SELECT pizza_id FROM pizzas ORDER BY RAND() LIMIT 1);
        CALL add_order_detail(@random_pizza, FLOOR(1 + RAND() * 5)); -- Random quantity between 1 and 5
        SET i = i + 1;
    END WHILE;
END;
//

DELIMITER ;

-- Generate 10 random orders
CALL generate_random_orders(10);  

-- Example of a trigger for the denormalized table about working hours 
-- first checking the min and the max working hours
DROP TRIGGER IF EXISTS check_working_hours;
DELIMITER //
CREATE TRIGGER check_working_hours 
AFTER INSERT ON order_details
FOR EACH ROW
BEGIN
	DECLARE order_time TIME;
	DECLARE order_date DATE;
	-- Get the order time and date
SET order_time = (SELECT time FROM orders WHERE order_id = NEW.order_id);
SET order_date = (SELECT date FROM orders WHERE order_id = NEW.order_id);
	IF order_time BETWEEN '10:00:00' AND '23:00:00' THEN-- Update the denormalized table with is_working_hours = 1 (TRUE)
		INSERT INTO pizza_order_analysis (order_id, date, time, order_details_id, quantity, pizza_id, pizza_type_id, size, price, name, category, ingredients, has_olives, season, is_working_hours)VALUES (NEW.order_id, order_date, order_time, NEW.order_details_id, NEW.quantity, NEW.pizza_id, (SELECT pizza_type_id FROM pizzas WHERE pizza_id = NEW.pizza_id), 
				(SELECT size FROM pizzas WHERE pizza_id = NEW.pizza_id), (SELECT price FROM pizzas WHERE pizza_id = NEW.pizza_id),(SELECT name FROM pizza_types WHERE pizza_type_id = (SELECT pizza_type_id FROM pizzas WHERE pizza_id = NEW.pizza_id)),
				(SELECT category FROM pizza_types WHERE pizza_type_id = (SELECT pizza_type_id FROM pizzas WHERE pizza_id = NEW.pizza_id)),(SELECT ingredients FROM pizza_types WHERE pizza_type_id = (SELECT pizza_type_id FROM pizzas WHERE pizza_id = NEW.pizza_id)),
				(SELECT IF(ingredients LIKE '%Olives%', 1, 0)), NULL, 1);ELSE
		-- Update the denormalized table with is_working_hours = 0 (FALSE)
		INSERT INTO pizza_order_analysis (order_id, date, time, order_details_id, quantity, pizza_id, pizza_type_id, size, price, name, category, ingredients, has_olives, season, is_working_hours)
		VALUES (NEW.order_id, order_date, order_time, NEW.order_details_id, NEW.quantity, NEW.pizza_id, (SELECT pizza_type_id FROM pizzas WHERE pizza_id = NEW.pizza_id), (SELECT size FROM pizzas WHERE pizza_id = NEW.pizza_id), (SELECT price FROM pizzas WHERE pizza_id = NEW.pizza_id),
				(SELECT name FROM pizza_types WHERE pizza_type_id = (SELECT pizza_type_id FROM pizzas WHERE pizza_id = NEW.pizza_id)),(SELECT category FROM pizza_types WHERE pizza_type_id = (SELECT pizza_type_id FROM pizzas WHERE pizza_id = NEW.pizza_id)),
				(SELECT ingredients FROM pizza_types WHERE pizza_type_id = (SELECT pizza_type_id FROM pizzas WHERE pizza_id = NEW.pizza_id)),(SELECT IF(ingredients LIKE '%olives%', 1, 0)), NULL, 0);
				-- Optional: Signal an error to decline the order
SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Order placed outside working hours and has been declined';END IF;
END; //
DELIMITER ;
-- Extract is SELECT, Transform is the condition for 'is_working_hours', Load is INSERT


-- Procedure that calculates monthly revenue
DROP PROCEDURE IF EXISTS calculate_monthly_revenue;
DELIMITER //
CREATE PROCEDURE calculate_monthly_revenue(
	IN target_month INT,
	IN target_year INT, 
    OUT total_revenue DOUBLE
) 
BEGIN
-- Calculate the total revenue for the specified month where orders were placed during working hours
	SELECT SUM(price * quantity) INTO total_revenue FROM pizza_order_analysis
	WHERE MONTH(date) = target_month  AND YEAR(date) = target_year AND working_hours = 1;
    END;
//
DELIMITER ;

CALL calculate_monthly_revenue(1, 2015, @revenue);
SELECT @revenue AS 'Total Revenue for January 2015';

-- -----------------------------------------------------------------------
-- DATA MART
-- Data Mart views are produced for each of the four hypothesis stated earlier.
-- -----------------------------------------------------------------------
-- Question 1. Olive Preference Hypothesis
-- Hypothesis: Customers prefer pizzas without olives.
SELECT COUNT(DISTINCT pizza_type_id) AS pizza_types_with_olives
FROM pizza_order_analysis
WHERE has_olives = 1;
-- 7 out of 32 pizza types contain olives

-- Calculate the percentages of pizzas with and without olives by month
DROP VIEW IF EXISTS olive_preference_percentage;
CREATE VIEW olive_preference_percentage AS
    SELECT 
        YEAR(date) AS order_year,
        MONTH(date) AS order_month,
        (SUM(CASE
            WHEN has_olives = 1 THEN quantity
            ELSE 0
        END) / SUM(quantity)) * 100 AS percentage_with_olives,
        (SUM(CASE
            WHEN has_olives = 0 THEN quantity
            ELSE 0
        END) / SUM(quantity)) * 100 AS percentage_without_olives
    FROM pizza_order_analysis
    GROUP BY order_year, order_month;

SELECT * FROM olive_preference_percentage
ORDER BY order_year, order_month;
-- Conclusion: Among all months, the percentage of all pizza sold with olives 
-- does not exceed 20% of quantity order shares. Thus, we conclude that the relatively low demand on 
-- pizzas with olives stays consistnt and the hypothesis is confirmed.

-- -----------------------------------------------------------------------
-- Question 2: When is the restaurant busiest?
-- Hypothesis: lunch and dinner time are the busiest. 
DROP VIEW IF EXISTS restaurant_busiest_times;
CREATE VIEW restaurant_busiest_times AS
    SELECT 
        COUNT(order_id) AS order_count,
        DAYOFWEEK(date) AS day_of_week,
        HOUR(time) AS order_hour,
        CASE
            WHEN HOUR(time) BETWEEN 12 AND 14 THEN 'Lunch'
            WHEN HOUR(time) BETWEEN 18 AND 20 THEN 'Dinner'
            ELSE 'Other'
        END AS time_period
    FROM pizza_order_analysis
    GROUP BY day_of_week , order_hour , time_period
    ORDER BY day_of_week , order_hour;

SELECT * FROM restaurant_busiest_times;
-- Conclusion: The hours that have the largest number of orders are lunch and dinner.
-- Hypothesis confirmed.

-- -----------------------------------------------------------------------
-- Question 3: Analyze Seasonal Popularity of the Top five Pizzas
-- Hypothesis: The top five most popular pizzas have higher sales in specific 
-- seasons, indicating a seasonal preference among customers.
DROP VIEW IF EXISTS seasonal_popularity_top_pizzas;
CREATE VIEW seasonal_popularity_top_pizzas AS
    SELECT p.name AS pizza_name, p.season, SUM(p.quantity) AS total_quantity_sold
    FROM pizza_order_analysis p JOIN
        (SELECT name FROM pizza_order_analysis
        GROUP BY name
        ORDER BY SUM(quantity) DESC
        LIMIT 5) AS top_pizzas ON p.name = top_pizzas.name
GROUP BY p.name , p.season
ORDER BY pizza_name;

SELECT * FROM seasonal_popularity_top_pizzas;
-- Conclusion: Top 1-3 and 5th pizzas are sold the most during spring, 
-- 4th is sold the most during winter. Hypothesis confirmed.

-- -----------------------------------------------------------------------
-- Question 4: Order Quantity and Price Hypothesis:
-- Hypothesis: Lower-priced pizzas are ordered in higher quantities.
DROP VIEW IF EXISTS pizza_price_quantity;
CREATE VIEW pizza_price_quantity AS
SELECT name AS pizza_name, AVG(price) AS average_price, SUM(quantity) AS total_quantity_ordered
FROM pizza_order_analysis
GROUP BY pizza_name;

SELECT * FROM pizza_price_quantity;

SELECT 
    (SUM((average_price - avg_price_mean) * (total_quantity_ordered - qty_mean)) / COUNT(*)) /
    (STDDEV_SAMP(average_price) * STDDEV_SAMP(total_quantity_ordered)) AS correlation_coefficient
FROM 
    (SELECT 
        average_price, 
        total_quantity_ordered, 
        (SELECT AVG(average_price) FROM pizza_price_quantity) AS avg_price_mean, 
        (SELECT AVG(total_quantity_ordered) FROM pizza_price_quantity) AS qty_mean
     FROM pizza_price_quantity) AS stats;
-- Conclusion: A negative correlation between pizza price and order quantity of -0.296 
-- was found (not strong, but still negative). Thus, the hypothesis of lower-priced pizzas 
-- ordered in higher quantities is confirmed.