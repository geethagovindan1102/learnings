-- Databricks notebook source
-- SQL Practice on TPCH Orders Table
-- Topic: Filtering, LIMIT, basic SELECT
-- Platform: Databricks

-- COMMAND ----------

-- # Task:
-- # Get all orders where the order status is 'O' (Open orders).
select * from samples.tpch.orders

limit 10

-- COMMAND ----------

-- Show order id, order date, total price
-- sorted by total price (highest first).
select 
o_orderkey,
o_orderdate,
o_totalprice
 from samples.tpch.orders
order by o_totalprice desc;

-- COMMAND ----------

-- Find the total revenue (sum of total price)
-- for each order status.
select o_orderstatus,
sum(o_totalprice)as totalrevenue
from samples.tpch.orders
group by o_orderstatus

-- COMMAND ----------

-- Task:
-- Find the average order price
-- for orders placed after 1995-01-01.
select avg(o_totalprice) as avgprice
 from samples.tpch.orders
 where o_orderdate > '1995-01-01'

-- COMMAND ----------

-- Find top 3 order priorities
-- with the highest total sales.
select o_orderpriority,
sum(o_totalprice) as totalsales
 from samples.tpch.orders
group by o_orderpriority
order by totalsales desc
limit 3

-- COMMAND ----------

-- Display order id, customer id, and total price,
-- and rename:
-- o_orderkey → order_id
-- o_custkey → customer_id
-- o_totalprice → total_price

select o_orderkey as order_id,
o_custkey as customer_id,
o_totalprice as total_price from samples.tpch.orders

-- COMMAND ----------

-- Find the number of orders per year.
select year(o_orderdate) as year,
count(*) as ordercount
 from samples.tpch.orders
 group by year

-- COMMAND ----------

-- Find order statuses where
-- the total sales is greater than 100 million.

select * from samples.tpch.orders
where o_totalprice > 100000000

-- COMMAND ----------

-- Find total sales for each
-- order status + order priority combination.

select 
  o_orderstatus,
  o_orderpriority,
  sum(o_totalprice) as total_sales
from samples.tpch.orders
group by o_orderstatus, o_orderpriority

-- COMMAND ----------

-- Find the top 5 customers
-- with the highest total order value.

select c.c_name,
sum(o.o_totalprice) as totalrevenue
from samples.tpch.orders o
join samples.tpch.customer c
on o.o_custkey = c.c_custkey
group by c.c_name
order by totalrevenue desc
limit 5

-- COMMAND ----------

-- Find unique customers
select distinct(o_custkey)as unique_customer from samples.tpch.orders

-- COMMAND ----------

-- Assign a rank to each order based on o_totalprice
-- (highest price = rank 1).
select o_totalprice,
rank()over(order by o_totalprice desc) as rank from samples.tpch.orders

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for i in range(10):
-- MAGIC     print("Processing record", i)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales = [100, 200, 150]
-- MAGIC
-- MAGIC total = 0
-- MAGIC for s in sales:
-- MAGIC     total += s
-- MAGIC
-- MAGIC display(total)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC numbers = [1, 2, 3]
-- MAGIC numbers.append(4)
-- MAGIC numbers.remove(1)
-- MAGIC display(numbers)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC marks = [45, 60, 80, 30]
-- MAGIC
-- MAGIC total = 0
-- MAGIC for m in marks:
-- MAGIC     total = total + m
-- MAGIC
-- MAGIC print(total)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Scenario 1: Student Status
-- MAGIC # You are given a student’s age.
-- MAGIC # Requirement:
-- MAGIC # If age is 18 or above, print "Eligible for exam"
-- MAGIC # Otherwise, print "Not eligible for exam"
-- MAGIC
-- MAGIC age = 18
-- MAGIC if age >= 18:
-- MAGIC     print("Eligible for exam")
-- MAGIC else:
-- MAGIC     print("not eligible for exam")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You have daily sales amounts for a shop stored in a list.
-- MAGIC # Requirement:
-- MAGIC # Calculate the total sales
-- MAGIC # Print the final total
-- MAGIC
-- MAGIC sales=[100,200,350,400]
-- MAGIC total = 0
-- MAGIC for sale in sales:
-- MAGIC     total = total + sale
-- MAGIC display(total)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- You have a list of numbers.
-- MAGIC # -- Requirement:
-- MAGIC # -- Loop through the list
-- MAGIC # -- If the number is greater than 5, print "High"
-- MAGIC # -- Otherwise, print "Low"
-- MAGIC number=[1,2,3,4,5,6,7,8,9,10]
-- MAGIC for i in number:
-- MAGIC   if i>5:
-- MAGIC     print("High")
-- MAGIC   else:
-- MAGIC     print("Low")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- You want to create a simple countdown.
-- MAGIC # -- Requirement:
-- MAGIC # -- Start from a number (example: 3)
-- MAGIC # -- Print "Processing" until the number reaches 0
-- MAGIC
-- MAGIC n = 3
-- MAGIC while n > 0:
-- MAGIC     print("Processing")
-- MAGIC     n = n- 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- A store checks stock count.
-- MAGIC # -- Requirement:
-- MAGIC # -- If stock count is 0, print "Out of stock"
-- MAGIC # -- If stock count is greater than 0, print "Stock available"
-- MAGIC stock = 33
-- MAGIC if stock == 0:
-- MAGIC     print("out of stock")
-- MAGIC else:
-- MAGIC     print("stock available")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Even or Odd Check
-- MAGIC # You are given a number.
-- MAGIC # Requirement:
-- MAGIC # If the number is even, print "Even"
-- MAGIC # Otherwise, print "Odd"
-- MAGIC
-- MAGIC n = 22
-- MAGIC if n % 2 == 0:
-- MAGIC     print("even")
-- MAGIC else:
-- MAGIC     print("odd")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # An employee’s salary is given.
-- MAGIC # Requirement:
-- MAGIC # If salary is greater than 30,000, add a bonus of 5,000
-- MAGIC # Otherwise, add a bonus of 2,000
-- MAGIC # Print the final salary
-- MAGIC
-- MAGIC salary = 12000
-- MAGIC if salary > 30000:
-- MAGIC     bonus = 5000
-- MAGIC else:
-- MAGIC     bonus = 2000
-- MAGIC print(salary + bonus)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- # A shopping cart has multiple item prices stored in a list.
-- MAGIC # -- # Requirement:
-- MAGIC # -- # Calculate the total price
-- MAGIC # -- # If total is greater than 1,000, print "Discount applicable"
-- MAGIC # -- # Otherwise, print "No discount"
-- MAGIC
-- MAGIC shoping = [ 50, 70, 59, 120, 100]
-- MAGIC total = 0
-- MAGIC for i in shoping:
-- MAGIC     total = total + i
-- MAGIC
-- MAGIC if total > 1000:
-- MAGIC     print("Discount applicable")
-- MAGIC else:
-- MAGIC     print("No discount")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You have a list of marks.
-- MAGIC # Requirement:
-- MAGIC # Loop through each mark
-- MAGIC # If mark is 50 or above, print "Pass"
-- MAGIC # Otherwise, print "Fail"
-- MAGIC
-- MAGIC mark = [20,50,70,90,50,30,12]
-- MAGIC for i in mark:
-- MAGIC     if i >= 50:
-- MAGIC         print("pass")
-- MAGIC     else:
-- MAGIC         print("fail")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # A system retries login attempts.
-- MAGIC # Requirement:
-- MAGIC # Start with attempts = 3
-- MAGIC # Print "Trying login" until attempts become 0
-- MAGIC
-- MAGIC attempts = 3
-- MAGIC while attempts > 0:
-- MAGIC     print("trying login")
-- MAGIC     attempts = attempts - 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Temperature value is given.
-- MAGIC # Requirement:
-- MAGIC # If temperature is above 30, print "Hot"
-- MAGIC # If temperature is between 20 and 30, print "Normal"
-- MAGIC # Otherwise, print "Cold"
-- MAGIC
-- MAGIC temp = 25
-- MAGIC if temp > 30:
-- MAGIC     print("hot")
-- MAGIC elif temp >= 20 and temp <= 30:
-- MAGIC     print("normal")
-- MAGIC else:
-- MAGIC     print("cold")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You have a list of numbers.
-- MAGIC # Requirement:
-- MAGIC # Add only positive numbers
-- MAGIC # Print the final sum
-- MAGIC
-- MAGIC number=[1,2,3,-5,-4,3,8,9,-2,33]
-- MAGIC sum=0
-- MAGIC for i in number:
-- MAGIC     if i > 0:
-- MAGIC         sum = sum + i
-- MAGIC         print(sum)
-- MAGIC         

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You are given a list of numbers.
-- MAGIC # Requirement:
-- MAGIC # Loop through the list
-- MAGIC # Find and print the largest number
-- MAGIC
-- MAGIC number=[2,4,6,7,9,8,4,2,11]
-- MAGIC for i in number:
-- MAGIC     if i == max(number):
-- MAGIC         print(i)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You have a list of numbers.
-- MAGIC # Requirement:
-- MAGIC # Count how many numbers are greater than 0
-- MAGIC # Print the count
-- MAGIC number=[5,-88,-11,2,4,-6,8,-30]
-- MAGIC count=0
-- MAGIC for i in number:
-- MAGIC     if i > 0:
-- MAGIC         count = count + 1
-- MAGIC         print(count)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- # You are looping through numbers from 1 to 10.
-- MAGIC # -- # Requirement:
-- MAGIC # -- # Stop the loop when the number reaches 6
-- MAGIC # -- # Print numbers before stopping
-- MAGIC for i in range(1, 11):
-- MAGIC     if i == 6:
-- MAGIC         break
-- MAGIC     print(i)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You are looping through a list of numbers.
-- MAGIC # Requirement:
-- MAGIC # Skip the number 5
-- MAGIC # Print all other numbers
-- MAGIC
-- MAGIC number=[2,5,4,6,8,5,9,44,5]
-- MAGIC for i in number:
-- MAGIC     if i == 5:
-- MAGIC         continue
-- MAGIC     print(i)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You are given a list of numbers.
-- MAGIC # Requirement:
-- MAGIC # Add numbers until you encounter 0
-- MAGIC # Stop the loop when 0 is found
-- MAGIC # Print the sum
-- MAGIC
-- MAGIC number=[1,2,4,5,7,8,0,11,22,55,1,2,3]
-- MAGIC count = 0
-- MAGIC for i in number:
-- MAGIC     if i == 0:
-- MAGIC         break
-- MAGIC     count = count + i
-- MAGIC     print(count)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # A list represents attendance (1 = present, 0 = absent).
-- MAGIC # Requirement:
-- MAGIC # Count how many students are present
-- MAGIC # Print the count
-- MAGIC list = [1,0,1,1,0,0,0,0,1,1]
-- MAGIC for i in list:
-- MAGIC     if i == 1:
-- MAGIC         print("present")
-- MAGIC     else:
-- MAGIC         print("absent")
-- MAGIC     
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # A system allows only 3 attempts.
-- MAGIC # Requirement:
-- MAGIC # Print "Attempting login"
-- MAGIC # Stop after 3 attempts
-- MAGIC
-- MAGIC for i in range(1,4):
-- MAGIC     print("attempting login")
-- MAGIC     if i == 3:
-- MAGIC         break

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You have a list of numbers.
-- MAGIC # Requirement:
-- MAGIC # Find and print the smallest number
-- MAGIC
-- MAGIC list = [11,44,66,3,9,77,2,1,0,-9]
-- MAGIC for i in list:
-- MAGIC     if i == min(list):
-- MAGIC         print(i)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You are given student details:
-- MAGIC # name
-- MAGIC # age
-- MAGIC # marks
-- MAGIC # Requirement:
-- MAGIC # Store them using a dictionary
-- MAGIC # Print only the student’s name and marks
-- MAGIC
-- MAGIC students = {
-- MAGIC     "name": "devi",
-- MAGIC     "age": 22,
-- MAGIC     "marks": 55
-- MAGIC }
-- MAGIC print(students["name"])
-- MAGIC print(students["marks"])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Update Product Price
-- MAGIC # You have a product with:
-- MAGIC # name
-- MAGIC # price
-- MAGIC # Requirement:
-- MAGIC # Update the price
-- MAGIC # Print the updated dictionary
-- MAGIC
-- MAGIC product = {
-- MAGIC     "name":"dress",
-- MAGIC     "price":500
-- MAGIC }
-- MAGIC product["price"]=600
-- MAGIC print(product)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # A dictionary contains:
-- MAGIC # product name
-- MAGIC # stock count
-- MAGIC # Requirement:
-- MAGIC # If stock is 0 → print "Out of stock"
-- MAGIC # Else → print "Available"
-- MAGIC product = {
-- MAGIC     "name":"shoes",
-- MAGIC     "stock":0
-- MAGIC }
-- MAGIC if product["stock"] == 0:
-- MAGIC     print("out of stock")
-- MAGIC else:
-- MAGIC     print("available")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You have a dictionary with multiple key–value pairs.
-- MAGIC # Requirement:
-- MAGIC # Loop through the dictionary
-- MAGIC # Print each key and its value
-- MAGIC house ={
-- MAGIC     "name":"divya",
-- MAGIC     "age":25,
-- MAGIC     "members":4
-- MAGIC }
-- MAGIC for h in house:
-- MAGIC     print(h, house[h])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You have employee information:
-- MAGIC # id
-- MAGIC # name
-- MAGIC # salary
-- MAGIC # Requirement:
-- MAGIC # Store details in a dictionary
-- MAGIC # Print the employee’s name and salary
-- MAGIC employee = {
-- MAGIC     "id":1,
-- MAGIC     "name":"geetha",
-- MAGIC     "salary":50000
-- MAGIC }
-- MAGIC
-- MAGIC print(employee["name"])
-- MAGIC print(employee["salary"])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You have a dictionary with:
-- MAGIC # employee name
-- MAGIC # salary
-- MAGIC # Requirement:
-- MAGIC # Increase the salary by 10%
-- MAGIC # Print the updated salary
-- MAGIC employee = {
-- MAGIC     "name":"geetha",
-- MAGIC     "salary":50000,
-- MAGIC }
-- MAGIC employee["salary"] = int(employee["salary"] * 1.10)
-- MAGIC print(employee["salary"])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You have a dictionary with:
-- MAGIC # student name
-- MAGIC # marks
-- MAGIC # Requirement:
-- MAGIC # If marks ≥ 50 → print "Pass"
-- MAGIC # Else → print "Fail"
-- MAGIC students = {
-- MAGIC     "name":"geetha",
-- MAGIC     "marks":50
-- MAGIC }
-- MAGIC if students["marks"]>= 50:
-- MAGIC     print("pass")
-- MAGIC else:
-- MAGIC     print("Fail")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You have a dictionary with:
-- MAGIC # product name
-- MAGIC # price
-- MAGIC # Requirement:
-- MAGIC # Add a new key called "category"
-- MAGIC # Print the full dictionary
-- MAGIC
-- MAGIC product = {
-- MAGIC     "name":"dress",
-- MAGIC     "price":500,
-- MAGIC }
-- MAGIC product["category"] = "clothing"
-- MAGIC print(product)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # You are given a dictionary.
-- MAGIC # Requirement:
-- MAGIC # Count how many keys are present
-- MAGIC # Print the count of present (1)
-- MAGIC
-- MAGIC attendance = {
-- MAGIC     "a": 1,
-- MAGIC     "b": 0,
-- MAGIC     "c": 1,
-- MAGIC     "d": 1,
-- MAGIC     "e": 1,
-- MAGIC     "f": 1,
-- MAGIC     "g": 0,
-- MAGIC     "h": 0,
-- MAGIC     "i": 0,
-- MAGIC     "j": 1
-- MAGIC }
-- MAGIC count = sum(1 for v in attendance.values() if v == 1)
-- MAGIC print(count)

-- COMMAND ----------

