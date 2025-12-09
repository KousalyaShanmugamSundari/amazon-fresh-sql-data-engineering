 Basic SQL Queries
--Task 3
--Retrieve all customers from a specific city

SELECT DISTINCT city FROM customers ORDER BY city;

SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM customers_staging;

--customers from "Adamville"
SELECT * FROM customers
WHERE city = 'Adamville';


--Fetch all products under the "Fruits" category:
SELECT * FROM products
WHERE category = 'fruits';

SELECT * FROM products
WHERE category Ilike 'fruits';

SELECT * FROM products
WHERE LOWER(category) = 'fruits';

---------------------------------------------------------------------------------------
Data Definition Language (DDL) and Constraints:
--Task:4

--Add Primary Key:
ALTER TABLE customers
ADD CONSTRAINT pk_customers PRIMARY KEY (customerid);

SELECT conname, conkey
FROM pg_constraint
WHERE conrelid = 'customers'::regclass
AND contype = 'p';


--Age cannot be NULL
ALTER TABLE customers
ALTER COLUMN age SET NOT NULL;

--Age must be > 18
ALTER TABLE customers
ADD CONSTRAINT chk_age CHECK (age > 18);

SELECT customerid, name, age
FROM customers
WHERE age IS NULL OR age <= 18;

UPDATE customers
SET age = 19
WHERE age IS NULL OR age <= 18;

ALTER TABLE customers
ADD CONSTRAINT chk_age CHECK (age > 18);


--Name must be UNIQUE
ALTER TABLE customers
ADD CONSTRAINT uq_customer_name UNIQUE (name);

SELECT name, COUNT(*)
FROM customers
GROUP BY name
HAVING COUNT(*) > 1;

DELETE FROM customers c
WHERE c.customerid IN (
    SELECT customerid
    FROM (
        SELECT customerid,
               ROW_NUMBER() OVER (PARTITION BY name ORDER BY customerid) AS rn
        FROM customers
    ) t
    WHERE t.rn > 1
);


ALTER TABLE customers
ADD CONSTRAINT uq_customer_name UNIQUE (name);


------------------------------------------------------------------------------------------------------------
Data Manipulation Language (DML)

--Task 5: Insert 3 new rows into the Products table using INSERT statements.

SELECT supplierid, suppliername FROM suppliers LIMIT 3;

"898ca408-050f-49be-b083-00eedf4bd064"
"e0ceb86c-2a85-4000-a0b7-4146a4bc51c4"
"158ae598-5c95-4dd7-b714-1f24332ddf9c"

INSERT INTO products (productid, productname, category, subcategory, priceperunit, stockquantity, supplierid)
VALUES 
    (gen_random_uuid(), 'Organic Apples', 'Fruits', 'Fresh Fruits', 3.50, 120, '898ca408-050f-49be-b083-00eedf4bd064'),
    (gen_random_uuid(), 'Brown Bread', 'Bakery', 'Whole Grain', 2.20, 75, 'e0ceb86c-2a85-4000-a0b7-4146a4bc51c4'),
    (gen_random_uuid(), 'Almond Milk', 'Beverages', 'Dairy Alternatives', 4.80, 50, '158ae598-5c95-4dd7-b714-1f24332ddf9c');


--Task 6: Update the stock quantity of a product where ProductID matches a specific ID.

SELECT productid, productname, stockquantity FROM products LIMIT 3;

"2aa28375-c563-41b5-aa33-8e2c2e0f4db9"
"e9282403-e234-4e35-a711-50acb03bbecc"
"d79d1b95-ecdf-4810-aea0-45e9bd10627d"

UPDATE products
SET stockquantity = 200
WHERE productid = '2aa28375-c563-41b5-aa33-8e2c2e0f4db9';

SELECT productid, productname, stockquantity
FROM products
WHERE productid = '2aa28375-c563-41b5-aa33-8e2c2e0f4db9';


--Task 7: Delete a supplier from the Suppliers table where their city matches a specific value.

SELECT DISTINCT city FROM suppliers;

DELETE FROM suppliers
WHERE LOWER(city) = LOWER('Cynthiatown');


-----------------------------------------------------------------------------------------------------------------

SQL Constraints and Operators

--Task 8: Use SQL constraints to:
--Add a CHECK constraint to ensure that ratings in the Reviews table are between 1 and 5.
--Add a DEFAULT constraint for the PrimeMember column in the Customers table (default value: "No").

SELECT * FROM reviews
WHERE rating < 1 OR rating > 5;

ALTER TABLE reviews
ADD CONSTRAINT chk_rating_range CHECK (rating BETWEEN 1 AND 5);

--If primemember is BOOLEAN, default should be:

ALTER TABLE customers
ALTER COLUMN primemember SET DEFAULT false;

--If the column is TEXT (your dataset may use 'Yes'/'No')

ALTER TABLE customers
ALTER COLUMN primemember SET DEFAULT 'No';

------------------------------------------------------------------------------------------------------------------
Clauses and Aggregations
--Task 9: Write queries using:
--WHERE clause to find orders placed after 2024-01-01.
--HAVING clause to list products with average ratings greater than 4.
--GROUP BY and ORDER BY clauses to rank products by total sales.

SELECT * FROM orders
WHERE orderdate > '2024-01-01';

SELECT 
    productid,
    AVG(rating) AS average_rating,
    COUNT(reviewid) AS total_reviews
FROM reviews
GROUP BY productid
HAVING AVG(rating) > 4;

SELECT 
    p.productid,
    p.productname,
    SUM(od.quantity * od.unitprice) AS total_sales
FROM products p
JOIN order_details od ON p.productid = od.productid
GROUP BY p.productid, p.productname
ORDER BY total_sales DESC;


--------------------------------------------------------------------------------------------------------------------

Task 10: Identifying High-Value Customers
Scenario:
--Amazon Fresh wants to identify top customers based on their total spending. We will:
--Calculate each customer's total spending.
--Rank customers based on their spending.
--Identify customers who have spent more than ₹5,000.


SELECT 
    c.customerid,
    c.name,
    SUM(o.totalamount) AS total_spending
FROM customers c
JOIN orders o ON c.customerid = o.customerid
GROUP BY c.customerid, c.name;

SELECT 
    c.customerid,
    c.name,
    SUM(o.totalamount) AS total_spending,
    RANK() OVER (ORDER BY SUM(o.totalamount) DESC) AS spending_rank
FROM customers c
JOIN orders o ON c.customerid = o.customerid
GROUP BY c.customerid, c.name
ORDER BY total_spending DESC;

SELECT 
    c.customerid,
    c.name,
    SUM(o.totalamount) AS total_spending
FROM customers c
JOIN orders o ON c.customerid = o.customerid
GROUP BY c.customerid, c.name
HAVING SUM(o.totalamount) > 5000
ORDER BY total_spending DESC;

----------------------------------------------------------------------------------------------

Complex Aggregations and Joins
--Task 11: Use SQL to:
--Join the Orders and OrderDetails tables to calculate total revenue per order.
--Identify customers who placed the most orders in a specific time period.
--Find the supplier with the most products in stock.


1) Calculate total revenue per order (join orders + order_details)
This joins order header and order lines, computes line_total = quantity * unitprice, sums per order.
-- total revenue per order (order header + order lines)

SELECT
  o.orderid,
  o.customerid,
  o.orderdate,
  SUM(od.quantity * od.unitprice)       AS order_revenue,
  COUNT(od.orderdetailid)               AS lines_in_order
FROM orders o
JOIN order_details od ON o.orderid = od.orderid
GROUP BY o.orderid, o.customerid, o.orderdate
ORDER BY order_revenue DESC;


2) Identify customers who placed the most orders in a specific time period
This counts orders per customer in a date range, ranks them and returns top customers. Replace the date bounds with your period.
-- Top customers by number of orders in a period (example: 2024-01-01 .. 2024-12-31)

WITH customer_order_counts AS (
  SELECT
    c.customerid,
    c.name,
    COUNT(o.orderid) AS num_orders
  FROM customers c
  JOIN orders o ON c.customerid = o.customerid
  WHERE o.orderdate >= DATE '2024-01-01'
    AND o.orderdate <  DATE '2025-01-01'   -- exclusive upper bound
  GROUP BY c.customerid, c.name
)
SELECT
  customerid,
  name,
  num_orders,
  RANK() OVER (ORDER BY num_orders DESC) AS orders_rank
FROM customer_order_counts
ORDER BY num_orders DESC
LIMIT 20;   -- show top 20 (adjust as needed)



--All customers tied for top place:
-- Find the maximum order count then list all customers with that count

WITH counts AS (
  SELECT c.customerid, c.name, COUNT(o.orderid) AS num_orders
  FROM customers c
  JOIN orders o ON c.customerid = o.customerid
  WHERE o.orderdate BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
  GROUP BY c.customerid, c.name
)
SELECT *
FROM counts
WHERE num_orders = (SELECT MAX(num_orders) FROM counts);



--3)Find the supplier with the most products in stock
It sums stockquantity per supplier and returns the supplier(s) with the highest total stock.
-- Supplier total stock, ordered descending

SELECT
  s.supplierid,
  s.suppliername,
  SUM(COALESCE(p.stockquantity,0)) AS total_stock
FROM suppliers s
LEFT JOIN products p ON s.supplierid = p.supplierid
GROUP BY s.supplierid, s.suppliername
ORDER BY total_stock DESC
LIMIT 1;


--All suppliers with the top total:

WITH supplier_stock AS (
  SELECT s.supplierid, s.suppliername, SUM(COALESCE(p.stockquantity,0)) AS total_stock
  FROM suppliers s
  LEFT JOIN products p ON s.supplierid = p.supplierid
  GROUP BY s.supplierid, s.suppliername
)
SELECT *
FROM supplier_stock
WHERE total_stock = (SELECT MAX(total_stock) FROM supplier_stock);

---------------------------------------------------------------------------------------------------------------------------

Normalization
--Task 12: Normalize the Products table to 3NF:
--Separate product categories and subcategories into a new table.
--Create foreign keys to maintain relationships.


-- 0) OPTIONAL: start transaction for safety
-- BEGIN;

-- 1) Backup products (Restores if anything goes wrong)
DROP TABLE IF EXISTS products_backup;
CREATE TABLE products_backup AS
SELECT * FROM products;

-- 2) Create categories table
DROP TABLE IF EXISTS categories CASCADE;
CREATE TABLE categories (
  categoryid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  category TEXT NOT NULL UNIQUE
);

-- 3) Create subcategories table
DROP TABLE IF EXISTS subcategories CASCADE;
CREATE TABLE subcategories (
  subcategoryid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  categoryid   UUID NOT NULL REFERENCES categories(categoryid) ON DELETE RESTRICT,
  subcategory  TEXT NOT NULL,
  CONSTRAINT uq_category_subcategory UNIQUE (categoryid, subcategory)
);

-- 4) Populate categories from distinct products.category
INSERT INTO categories (category)
SELECT DISTINCT TRIM(category) AS category
FROM products
WHERE category IS NOT NULL AND TRIM(category) <> ''
ON CONFLICT (category) DO NOTHING;

-- 5) Populate subcategories from distinct (category, subcategory)
INSERT INTO subcategories (categoryid, subcategory)
SELECT c.categoryid, TRIM(p.subcategory) AS subcategory
FROM (
  SELECT DISTINCT TRIM(category) AS category, TRIM(subcategory) AS subcategory
  FROM products
  WHERE subcategory IS NOT NULL AND TRIM(subcategory) <> ''
) p
JOIN categories c ON LOWER(c.category) = LOWER(p.category)
ON CONFLICT (categoryid, subcategory) DO NOTHING;

-- 6) Add subcategoryid column to products (nullable for now)
ALTER TABLE products
ADD COLUMN subcategoryid UUID;

-- 7) Update products.subcategoryid by matching category + subcategory (case-insensitive, trim)
UPDATE products p
SET subcategoryid = s.subcategoryid
FROM subcategories s
JOIN categories c ON s.categoryid = c.categoryid
WHERE LOWER(TRIM(p.category)) = LOWER(TRIM(c.category))
  AND LOWER(TRIM(p.subcategory)) = LOWER(TRIM(s.subcategory))
  AND (p.subcategoryid IS NULL OR p.subcategoryid <> s.subcategoryid);

-- 8) Verify how many products are still without subcategoryid
SELECT COUNT(*) AS unmatched_products
FROM products
WHERE subcategoryid IS NULL;

-- If unmatched_products > 0, inspect them:
SELECT productid, productname, category, subcategory FROM products WHERE subcategoryid IS NULL LIMIT 50;

-- 9) (Optional) For products that had category but no subcategory,
-- you could map them to a default subcategory under their category.
-- Example: create a "General" subcategory per category and assign those rows:
-- Note: run only if you want this behavior.

-- -- create 'General' subcategories for categories that lack a specific match
INSERT INTO subcategories (categoryid, subcategory)
SELECT c.categoryid, 'General'
FROM categories c
WHERE NOT EXISTS (
   SELECT 1 FROM subcategories s WHERE s.categoryid = c.categoryid AND LOWER(TRIM(s.subcategory)) = 'general'
 );
 
-- -- update remaining products (category matched but subcategory blank) to this 'General'
UPDATE products p
SET subcategoryid = s.subcategoryid
FROM subcategories s
JOIN categories c ON s.categoryid = c.categoryid
WHERE LOWER(TRIM(p.category)) = LOWER(TRIM(c.category))
AND (p.subcategory IS NULL OR TRIM(p.subcategory) = '')
AND LOWER(TRIM(s.subcategory)) = 'general';

-- 10) After you are satisfied that subcategoryid is populated for all products,
-- add FK and make column NOT NULL (or keep nullable if you want to allow uncategorized)
-- First ensure there are no nulls:
SELECT COUNT(*) FROM products WHERE subcategoryid IS NULL;

-- how many products still missing subcategoryid
SELECT COUNT(*) AS unmatched_count
FROM products
WHERE subcategoryid IS NULL;

-- show sample unmatched rows to inspect
SELECT productid, productname, category, subcategory
FROM products
WHERE subcategoryid IS NULL
LIMIT 100;

SELECT DISTINCT
  TRIM(category) AS category,
  TRIM(subcategory) AS subcategory,
  COUNT(*) AS rows
FROM products
WHERE subcategoryid IS NULL
GROUP BY TRIM(category), TRIM(subcategory)
ORDER BY COUNT(*) DESC;


Create General subcategory for every category that lacks it:

-- create 'General' subcategory where missing
INSERT INTO subcategories (categoryid, subcategory)
SELECT c.categoryid, 'General'
FROM categories c
WHERE NOT EXISTS (
  SELECT 1 FROM subcategories s
  WHERE s.categoryid = c.categoryid
    AND LOWER(TRIM(s.subcategory)) = 'general'
);

-- 1) create 'General' subcategories for categories that lack it (safe to run repeatedly)
INSERT INTO subcategories (categoryid, subcategory)
SELECT c.categoryid, 'General'
FROM categories c
WHERE NOT EXISTS (
  SELECT 1
  FROM subcategories s
  WHERE s.categoryid = c.categoryid
    AND LOWER(TRIM(s.subcategory)) = 'general'
);

-- 2) update products that have matching category but NULL/blank subcategory,
--    assigning them the 'General' subcategory for that category
UPDATE products p
SET subcategoryid = s.subcategoryid
FROM subcategories s
JOIN categories c ON s.categoryid = c.categoryid
WHERE LOWER(TRIM(s.subcategory)) = 'general'
  AND LOWER(TRIM(p.category)) = LOWER(TRIM(c.category))
  AND p.subcategoryid IS NULL
  AND (p.subcategory IS NULL OR TRIM(p.subcategory) = '');


-- how many still unmatched
SELECT COUNT(*) AS unmatched_after FROM products WHERE subcategoryid IS NULL;

-- sample remaining unmatched rows (if any)
SELECT productid, productname, category, subcategory
FROM products
WHERE subcategoryid IS NULL
LIMIT 50;

INSERT INTO categories (category)
SELECT 'Uncategorized'
WHERE NOT EXISTS (
  SELECT 1 FROM categories c
  WHERE LOWER(TRIM(c.category)) = 'uncategorized'
);

INSERT INTO subcategories (categoryid, subcategory)
SELECT c.categoryid, 'General'
FROM categories c
WHERE LOWER(TRIM(c.category)) = 'uncategorized'
  AND NOT EXISTS (
    SELECT 1 FROM subcategories s
    WHERE s.categoryid = c.categoryid
      AND LOWER(TRIM(s.subcategory)) = 'general'
);

UPDATE products p
SET subcategoryid = s.subcategoryid
FROM subcategories s
JOIN categories c ON s.categoryid = c.categoryid
WHERE LOWER(TRIM(c.category)) = 'uncategorized'
  AND LOWER(TRIM(s.subcategory)) = 'general'
  AND p.subcategoryid IS NULL
  -- restrict to products that truly lack category/subcategory text:
  AND (p.category IS NULL OR TRIM(p.category) = '')
  AND (p.subcategory IS NULL OR TRIM(p.subcategory) = '');

SELECT COUNT(*) AS unmatched_after FROM products WHERE subcategoryid IS NULL;

-- show any remaining sample rows (if any)
SELECT productid, productname, category, subcategory
FROM products
WHERE subcategoryid IS NULL
LIMIT 50;

ALTER TABLE products
  ALTER COLUMN subcategoryid SET NOT NULL;

ALTER TABLE products
  ADD CONSTRAINT fk_products_subcategory
  FOREIGN KEY (subcategoryid) REFERENCES subcategories(subcategoryid) ON DELETE RESTRICT;

-- quick sanity checks
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM categories;
SELECT COUNT(*) FROM subcategories;

-- sample join to confirm category/subcategory populated
SELECT p.productid, p.productname, c.category, s.subcategory
FROM products p
LEFT JOIN subcategories s ON p.subcategoryid = s.subcategoryid
LEFT JOIN categories c ON s.categoryid = c.categoryid
LIMIT 50;

SELECT COUNT(*) FROM products WHERE LOWER(TRIM(productname)) = 'unknown product';

------------------------------------------------------------------------------------------------------------------------------
Subqueries and Nested Queries
--Task 13: Write a subquery to:
--Identify the top 3 products based on sales revenue.

SELECT productid,
       (SELECT productname 
        FROM products p 
        WHERE p.productid = od.productid) AS productname,
       SUM(od.quantity * od.unitprice) AS total_revenue
FROM order_details od
GROUP BY productid
ORDER BY total_revenue DESC
LIMIT 3;


--Find customers who haven’t placed any orders yet.

SELECT c.*
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 
    FROM orders o
    WHERE o.customerid = c.customerid
);


-------------------------------------------------------------------------------------------------------------------------------------

Real-World Analysis
Task 14: Provide actionable insights:
--Which cities have the highest concentration of Prime members?
--What are the top 3 most frequently ordered categories?


SELECT 
    city,
    COUNT(*) AS total_customers,
    COUNT(*) FILTER (WHERE primemember = true) AS prime_customers,
    ROUND(
        (COUNT(*) FILTER (WHERE primemember = true)::DECIMAL 
        / NULLIF(COUNT(*), 0)) * 100, 2
    ) AS prime_percentage
FROM customers
GROUP BY city
ORDER BY prime_percentage DESC, prime_customers DESC;


SELECT 
    c.category,
    COUNT(*) AS order_count
FROM order_details od
JOIN products p ON od.productid = p.productid
JOIN subcategories s ON p.subcategoryid = s.subcategoryid
JOIN categories c ON s.categoryid = c.categoryid
GROUP BY c.category
ORDER BY order_count DESC
LIMIT 3;




