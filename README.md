# Data Engineering 1 - Term project 1: Pizza sales data
This repository contains Term Project 1 for the Data Engineering 1 course, involving a MySQL-based data pipeline to create operational and analytical data layers, an ETL process, and data marts using a pizza sales datasets.


## Operational layer
This database is a year's worth of sales from a from a fictitious pizza place Plato's Pizza, a Greek-inspired pizzeria.
The dataset has 4 tables:
1. The order_details table has 48621 rows containing order details regarding pizza type and order quantity.
2. The orders table record the datetime indicators of the 21351 orders.
3. The pizza_types table specifies the category, ingredients information about the 33 different pizza types offered by the pizza place.
4. The pizzas table has 97 rows containing the pricing details of pizza based on the size and pizza type.


## Analytics and Results
Plan of executed analytics:
1. **Olive Preference Hypothesis**

_Hypothesis_: Customers prefer pizzas without olives.

_Approach_: Compare the total quantity and percentages of pizzas ordered with and without olives as an ingredient (by months). 

_Conclusion_: Among all months, the percentage of all pizza sold with olives does not exceed 20% of quantity order shares. Thus, we conclude that the relatively low demand on pizzas with olives stays consistnt and the hypothesis is confirmed.

2. **When is the restaurant busiest?**

_Hypothesis_: lunch and dinner time are the busiest. 

_Approach_: Aggregate the number of orders during lunch (12:00 PM - 2:00 PM) and dinner (6:00 PM - 9:00 PM) hours and compare them with other hours of the day. Within the ETL, include a trigger that reacts if an order is made outside working hours (described more in the script etl_and_data_mart).

_Conclusion_: The hours that have the largest number of orders are lunch and dinner. Hypothesis confirmed.

3. **Analyze Seasonal Popularity of the Top Five Pizzas**

_Hypothesis_: The top five most popular pizzas have higher sales in specific seasons, indicating a seasonal preference among customers.

_Approach_: Using data mart views, compare the order quantities of the top three pizzas across different seasons to identify any significant seasonal trends in their popularity.

_Conclusion_: Top 1-3 and 5th pizzas are sold the most during spring, 4th is sold the most during winter. Hypothesis confirmed.

4. **Order Quantity and Price Hypothesis**

_Hypothesis_: Lower-priced pizzas are ordered in higher quantities.

_Approach_: Perform a correlation analysis between pizza price and order quantity. A negative correlation would support this hypothesis.

_Conclusion_: A negative correlation between pizza price and order quantity of -0.296 was found (not strong, but still negative). Thus, the hypothesis of lower-priced pizzas ordered in higher quantities is confirmed.

Note: the hypothesis testing was aimed to be as efficient as possible within the scope of data.
