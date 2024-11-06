-- Data Engineering 1: Term Project 1
-- Tatyana Yakushina
-- Script 3 out of 3: ETL PIPLINE and DATA MART
-- This script creates an ETL pipeline including Triggers and Stored procedures.
-- Data Mart views are produced for each of the four hypothesis stated earlier.
-- -----------------------------------------------------------------------

-- -----------------------------------------------------------------------
-- ETL PIPLINE
-- -----------------------------------------------------------------------
-- Below two procedures and two triggers are created and tested. 

-- A stored procedure is defined to add new order details, automatically 
-- assigning unique IDs and inserting associated entries into the orders 
-- and order_details tables. One trigger is defined to verify existence of 
-- order_id and pizza_id before insertions, ensuring data integrity.

-- Procedure to store the number of orders (needed later for the trigger)
DROP PROCEDURE IF EXISTS add_order_detail
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
END;//
DELIMITER ;
-- Extract is SELECT, Transformation is adding +1 and Loading is INSERT INTO

-- Trigger: 
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
SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Pizza ID does not exist in pizzas table';
END IF;
END;
//

DELIMITER;

-- Testing the trigger and procedure:
-- Test 1
-- Successfully passed the test
CALL add_order_detail('big_meat_s', 2);

-- Test 2
-- This fails, because there is no such pizza as 'tanya' :(
-- CALL add_order_detail('tanya', 10);

-- The procedure is not very efficient, because MySql has auto increment option, 
-- but this procedure was only for an example.


-- Example of a trigger for the denormalized table about working hours 
-- first check the min and the max working hours
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
DELIMITER //
CREATE PROCEDURE calculate_monthly_revenue(
	IN target_month INT,
	IN target_year INT, 
    OUT total_revenue DOUBLE
) 
BEGIN
-- Calculate the total revenue for the specified month and year-- where orders were placed during working hours
	SELECT SUM(price * quantity) INTO total_revenue FROM pizza_order_analysis
	WHERE MONTH(date) = target_month  AND YEAR(date) = target_year AND working_hours = 1;
    END;
//
DELIMITER ;

CALL calculate_monthly_revenue(1, 2015, @revenue);SELECT @revenue AS 'Total Revenue for January 2015';

-- -----------------------------------------------------------------------
-- DATA MART
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
-- Answer: Among all month, the percentage of all pizza sold with olives 
-- does not exceed 20%. Thus, 

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
-- Answer: The hours that have the largest number of orders are lunch and dinner.
-- Hypothesis confirmed.


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
-- Answer: Top 1-3 and 5th pizzas are sold the most during spring, 
-- 4th is sold the most during winter. Hypothesis partially confirmed.

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
-- Answer: A negative correlation of -0.296 was found. Thus, the hypothesis of 
-- lower-priced pizzas ordered in higher quantities is confirmed.