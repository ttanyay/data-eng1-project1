-- Data Engineering 1: Term Project 1
-- Tatyana Yakushina
-- Script 2 out of 3: ANALYTICS and ANALYTICAL LAYER
-- This script describes a plan of executed analytics and produces a denormalized data table 
-- using the operational layer. 
-- -----------------------------------------------------------------------

-- -----------------------------------------------------------------------
-- ANALYTICS
-- -----------------------------------------------------------------------
-- Test the following hypothesis:
-- 1. Olive Preference Hypothesis
-- Hypothesis: Customers prefer pizzas without olives
-- Approach: Compare the total quantity and percentages of pizzas ordered with 
-- and without olives as an ingredient (over the whole year and by months). 

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

-- Comment: the hypothesis testing potentially was aiming to be as efficient as possible 
-- within the scope of the scope of data.


-- -----------------------------------------------------------------------
-- ANALYTICAL LAYER
-- -----------------------------------------------------------------------
-- Create a denormalized table 'pizza_order_analysis' for analysis purposes
DROP TABLE IF EXISTS pizza_order_analysis;
CREATE TABLE pizza_order_analysis AS
SELECT orders.order_id, date, time, order_details_id, 
    quantity, order_details.pizza_id, pizzas.pizza_type_id, 
    size, price, name, ingredients, 
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
