ROLLBACK; -- clean any aborted transaction first
-- DROP final + staging + problem tables (safe if starting fresh)
DROP TABLE IF EXISTS order_details_problem_rows;
DROP TABLE IF EXISTS order_details_staging;
DROP TABLE IF EXISTS reviews_staging;
DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS customers_staging;

DROP TABLE IF EXISTS order_details;
DROP TABLE IF EXISTS reviews;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS suppliers;

----------------------------------------------------------------------------

-- Final typed schema (run once)
CREATE TABLE suppliers (
  supplierid UUID PRIMARY KEY,
  suppliername TEXT NOT NULL,
  contactperson TEXT,
  phone TEXT,
  city TEXT,
  state TEXT
);

CREATE TABLE products (
  productid UUID PRIMARY KEY,
  productname TEXT NOT NULL,
  category TEXT,
  subcategory TEXT,
  priceperunit NUMERIC(12,2),
  stockquantity INTEGER,
  supplierid UUID REFERENCES suppliers(supplierid) ON DELETE SET NULL
);

CREATE TABLE customers (
  customerid UUID PRIMARY KEY,
  name TEXT NOT NULL,
  age INTEGER,
  gender TEXT,
  city TEXT,
  state TEXT,
  country TEXT,
  signupdate DATE,
  primemember BOOLEAN
);

CREATE TABLE orders (
  orderid UUID PRIMARY KEY,
  customerid UUID REFERENCES customers(customerid) ON DELETE CASCADE,
  orderdate DATE,
  shipdate DATE,
  shipmode TEXT,
  totalamount NUMERIC(12,2)
);

CREATE TABLE order_details (
  orderdetailid UUID PRIMARY KEY,
  orderid UUID REFERENCES orders(orderid) ON DELETE CASCADE,
  productid UUID REFERENCES products(productid) ON DELETE SET NULL,
  quantity INTEGER,
  unitprice NUMERIC(12,2),
  discount NUMERIC(5,2)
);

CREATE TABLE reviews (
  reviewid UUID PRIMARY KEY,
  productid UUID REFERENCES products(productid) ON DELETE CASCADE,
  customerid UUID REFERENCES customers(customerid) ON DELETE SET NULL,
  rating INTEGER,
  reviewtext TEXT
);

----------------------------------------------------------------------------------------------


-- Staging tables (text) — import CSVs into these
CREATE TABLE customers_staging (
  customerid TEXT, name TEXT, age TEXT, gender TEXT, city TEXT, state TEXT, country TEXT, signupdate TEXT, primemember TEXT
);

CREATE TABLE orders_staging (
  orderid TEXT, customerid TEXT, orderdate TEXT, shipdate TEXT, shipmode TEXT, totalamount TEXT
);

CREATE TABLE products_staging (
  productid TEXT, productname TEXT, category TEXT, subcategory TEXT, priceperunit TEXT, stockquantity TEXT, supplierid TEXT
);

CREATE TABLE suppliers_staging (
  supplierid TEXT, suppliername TEXT, contactperson TEXT, phone TEXT, city TEXT, state TEXT
);

CREATE TABLE order_details_staging (
  orderdetailid TEXT, orderid TEXT, productid TEXT, quantity TEXT, unitprice TEXT, discount TEXT
);

CREATE TABLE reviews_staging (
  reviewid TEXT, productid TEXT, customerid TEXT, rating TEXT, reviewtext TEXT
);

------------------------------------------------------------------------------------------------------------------------------------------

-- 4A insert suppliers (simple cast)
BEGIN;
INSERT INTO suppliers (supplierid, suppliername, contactperson, phone, city, state)
SELECT
  trim(supplierid)::uuid,
  suppliername,
  contactperson,
  phone,
  city,
  state
FROM suppliers_staging
WHERE supplierid IS NOT NULL AND trim(supplierid) <> ''
ON CONFLICT (supplierid) DO NOTHING;
COMMIT;

-- Check
SELECT count(*) AS suppliers_count FROM suppliers;

-------------------------------------------------------------------------------

-- ensure extension for gen_random_uuid (if allowed). If permission denied, ignore the error and continue.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Insert missing supplier placeholders (distinct, trimmed)
INSERT INTO suppliers (supplierid, suppliername)
SELECT DISTINCT trim(ps.supplierid)::uuid, 'UNKNOWN SUPPLIER'
FROM products_staging ps
LEFT JOIN suppliers s ON trim(ps.supplierid)::text = s.supplierid::text
WHERE ps.supplierid IS NOT NULL AND trim(ps.supplierid) <> '' AND s.supplierid IS NULL;

-- 2) Insert products
BEGIN;
INSERT INTO products (productid, productname, category, subcategory, priceperunit, stockquantity, supplierid)
SELECT
  trim(productid)::uuid,
  productname,
  category,
  subcategory,
  NULLIF(trim(priceperunit),'')::numeric,
  NULLIF(trim(stockquantity),'')::int,
  CASE WHEN trim(coalesce(supplierid,'')) = '' THEN NULL ELSE trim(supplierid)::uuid END
FROM products_staging
WHERE productid IS NOT NULL AND trim(productid) <> ''
ON CONFLICT (productid) DO NOTHING;
COMMIT;

-- Checks
SELECT count(*) AS products_count FROM products;
SELECT count(*) AS suppliers_count_after_products FROM suppliers;

-------------------------------------------------------------------------------------------------------------

BEGIN;
-- trim staging
UPDATE customers_staging
SET customerid = trim(customerid), name = trim(coalesce(name,'')), signupdate = trim(coalesce(signupdate,'')), primemember = trim(coalesce(primemember,''));

-- insert converted customers
INSERT INTO customers (customerid, name, age, gender, city, state, country, signupdate, primemember)
SELECT
  trim(customerid)::uuid,
  name,
  NULLIF(trim(age),'')::int,
  NULLIF(gender,''),
  NULLIF(city,''),
  NULLIF(state,''),
  NULLIF(country,''),
  CASE WHEN signupdate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(signupdate,'MM/DD/YYYY') ELSE NULL END,
  CASE WHEN lower(primemember) IN ('yes','y','true','1') THEN true WHEN lower(primemember) IN ('no','n','false','0','') THEN false ELSE NULL END
FROM customers_staging
WHERE customerid IS NOT NULL AND trim(customerid) <> ''
ON CONFLICT (customerid) DO NOTHING;
COMMIT;

-- check
SELECT count(*) AS customers_count FROM customers;


--------------------------------------------------------------------------------------------------------------

BEGIN;
-- trim staging
UPDATE customers_staging
SET customerid = trim(customerid), name = trim(coalesce(name,'')), signupdate = trim(coalesce(signupdate,'')), primemember = trim(coalesce(primemember,''));

-- insert converted customers
INSERT INTO customers (customerid, name, age, gender, city, state, country, signupdate, primemember)
SELECT
  trim(customerid)::uuid,
  name,
  NULLIF(trim(age),'')::int,
  NULLIF(gender,''),
  NULLIF(city,''),
  NULLIF(state,''),
  NULLIF(country,''),
  CASE WHEN signupdate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(signupdate,'MM/DD/YYYY') ELSE NULL END,
  CASE WHEN lower(primemember) IN ('yes','y','true','1') THEN true WHEN lower(primemember) IN ('no','n','false','0','') THEN false ELSE NULL END
FROM customers_staging
WHERE customerid IS NOT NULL AND trim(customerid) <> ''
ON CONFLICT (customerid) DO NOTHING;
COMMIT;

-- check
SELECT count(*) AS customers_count FROM customers;


---------------------------------------------------------------------------------------------------------------------------------------------------------
-- create missing customer placeholders for any customerid referenced in orders_staging but missing in customers
INSERT INTO customers (customerid, name)
SELECT DISTINCT trim(os.customerid)::uuid, 'UNKNOWN CUSTOMER'
FROM orders_staging os
LEFT JOIN customers c ON trim(os.customerid)::text = c.customerid::text
WHERE os.customerid IS NOT NULL AND trim(os.customerid) <> '' AND c.customerid IS NULL;

-- insert orders
BEGIN;
INSERT INTO orders (orderid, customerid, orderdate, shipdate, shipmode, totalamount)
SELECT
  trim(orderid)::uuid,
  trim(customerid)::uuid,
  CASE WHEN orderdate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(orderdate,'MM/DD/YYYY') ELSE NULL END,
  CASE WHEN shipdate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(shipdate,'MM/DD/YYYY') ELSE NULL END,
  NULLIF(trim(shipmode),''),
  NULLIF(trim(totalamount),'')::numeric
FROM orders_staging
WHERE orderid IS NOT NULL AND trim(orderid) <> ''
ON CONFLICT (orderid) DO NOTHING;
COMMIT;

-- check
SELECT count(*) AS orders_count FROM orders;

--------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS order_details_problem_rows;
CREATE TABLE order_details_problem_rows AS
SELECT *
FROM order_details_staging
WHERE NOT (trim(orderdetailid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

SELECT count(*) AS problem_rows_backed_up FROM order_details_problem_rows;

--------------------------------------------------------------------------------------------------------------------------

UPDATE order_details_staging
SET orderdetailid = gen_random_uuid()::text
WHERE NOT (trim(orderdetailid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

-- check none left
SELECT count(*) AS still_bad_orderdetailid FROM order_details_staging
WHERE NOT (trim(orderdetailid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

------------------------------------------------------------------------------------------------------------------------------------------------

DELETE FROM order_details_staging
WHERE NOT (trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

SELECT count(*) AS staging_remaining_rows FROM order_details_staging;
SELECT count(*) AS problem_rows_saved FROM order_details_problem_rows;

--------------------------------------------------------------------------------------------------------------------------------

INSERT INTO orders (orderid, customerid)
SELECT DISTINCT trim(ods.orderid)::uuid, NULL::uuid
FROM order_details_staging ods
LEFT JOIN orders o ON trim(ods.orderid)::text = o.orderid::text
WHERE trim(ods.orderid) <> '' AND o.orderid IS NULL;

----------------------------------------------------------------------------------------------------------------

INSERT INTO order_details (orderdetailid, orderid, productid, quantity, unitprice, discount)
SELECT
  trim(orderdetailid)::uuid,
  trim(orderid)::uuid,
  trim(productid)::uuid,
  NULLIF(trim(quantity),'')::int,
  NULLIF(trim(unitprice),'')::numeric,
  NULLIF(trim(discount),'')::numeric
FROM order_details_staging
WHERE orderdetailid IS NOT NULL AND trim(orderdetailid) <> ''
  AND trim(orderid) IN (SELECT orderid::text FROM orders)
  AND trim(productid) IN (SELECT productid::text FROM products)
ON CONFLICT (orderdetailid) DO NOTHING;

-- verify
SELECT count(*) AS order_details_count FROM order_details;

----------------------------------------------------------------------------------------------------------------------------

-- backup problematic reviews
DROP TABLE IF EXISTS reviews_problem_rows;
CREATE TABLE reviews_problem_rows AS
SELECT *
FROM reviews_staging
WHERE NOT (trim(reviewid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

-- fix bad reviewid by generating UUIDs
UPDATE reviews_staging
SET reviewid = gen_random_uuid()::text
WHERE NOT (trim(reviewid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

-- remove rows with invalid productid/customerid (we kept them in problem table)
DELETE FROM reviews_staging
WHERE NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

-- insert cleaned reviews
INSERT INTO reviews (reviewid, productid, customerid, rating, reviewtext)
SELECT
  trim(reviewid)::uuid,
  trim(productid)::uuid,
  trim(customerid)::uuid,
  NULLIF(trim(rating),'')::int,
  reviewtext
FROM reviews_staging
WHERE reviewid IS NOT NULL AND trim(reviewid) <> ''
  AND trim(productid) IN (SELECT productid::text FROM products)
  AND (trim(customerid) = '' OR trim(customerid) IN (SELECT customerid::text FROM customers))
ON CONFLICT (reviewid) DO NOTHING;

SELECT count(*) AS reviews_count FROM reviews;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS suppliers_staging;
DROP TABLE IF EXISTS order_details_staging;
DROP TABLE IF EXISTS reviews_staging;

-- keep problem tables until you inspect them or remove them:
-- DROP TABLE IF EXISTS order_details_problem_rows;
-- DROP TABLE IF EXISTS reviews_problem_rows;


------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE customers_staging (
  customerid TEXT, name TEXT, age TEXT, gender TEXT, city TEXT, state TEXT, country TEXT, signupdate TEXT, primemember TEXT
);

CREATE TABLE orders_staging (
  orderid TEXT, customerid TEXT, orderdate TEXT, shipdate TEXT, shipmode TEXT, totalamount TEXT
);

CREATE TABLE products_staging (
  productid TEXT, productname TEXT, category TEXT, subcategory TEXT, priceperunit TEXT, stockquantity TEXT, supplierid TEXT
);

CREATE TABLE suppliers_staging (
  supplierid TEXT, suppliername TEXT, contactperson TEXT, phone TEXT, city TEXT, state TEXT
);

CREATE TABLE order_details_staging (
  orderdetailid TEXT, orderid TEXT, productid TEXT, quantity TEXT, unitprice TEXT, discount TEXT
);

CREATE TABLE reviews_staging (
  reviewid TEXT, productid TEXT, customerid TEXT, rating TEXT, reviewtext TEXT
);


-----------------------------------------------------------------------------------------------------------------------------------------


-- 1) back up rows with invalid supplierid (so nothing is lost)
DROP TABLE IF EXISTS suppliers_problem_rows;
CREATE TABLE suppliers_problem_rows AS
SELECT *
FROM suppliers_staging
WHERE NOT (trim(supplierid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

-- how many problem rows we backed up
SELECT count(*) AS problem_rows_backed_up FROM suppliers_problem_rows;

-- 2) insert rows with valid UUID supplierid into final suppliers table
BEGIN;
INSERT INTO suppliers (supplierid, suppliername, contactperson, phone, city, state)
SELECT
  trim(supplierid)::uuid,
  suppliername,
  contactperson,
  phone,
  city,
  state
FROM suppliers_staging
WHERE trim(supplierid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND supplierid IS NOT NULL
ON CONFLICT (supplierid) DO NOTHING;
COMMIT;

-- 3) verification / counts
SELECT
  (SELECT count(*) FROM suppliers)           AS suppliers_in_final,
  (SELECT count(*) FROM suppliers_staging)   AS suppliers_in_staging,
  (SELECT count(*) FROM suppliers_problem_rows) AS suppliers_problem_rows;


------------------------------------------------------------------------------------------------------------------------

-- Backup rows with invalid productid or invalid supplierid
DROP TABLE IF EXISTS products_problem_rows;
CREATE TABLE products_problem_rows AS
SELECT *
FROM products_staging
WHERE NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR (supplierid IS NOT NULL AND trim(supplierid) <> '' AND NOT (trim(supplierid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'));

-- Show how many were backed up
SELECT count(*) AS products_problem_rows FROM products_problem_rows;

-- Insert missing suppliers as placeholders (only if needed)
INSERT INTO suppliers (supplierid, suppliername)
SELECT DISTINCT trim(ps.supplierid)::uuid, 'UNKNOWN SUPPLIER'
FROM products_staging ps
LEFT JOIN suppliers s ON trim(ps.supplierid)::text = s.supplierid::text
WHERE ps.supplierid IS NOT NULL AND trim(ps.supplierid) <> '' AND s.supplierid IS NULL;

-- Insert valid products
BEGIN;
INSERT INTO products (productid, productname, category, subcategory, priceperunit, stockquantity, supplierid)
SELECT
  trim(productid)::uuid,
  productname,
  category,
  subcategory,
  NULLIF(trim(priceperunit),'')::numeric,
  NULLIF(trim(stockquantity),'')::int,
  CASE 
    WHEN trim(coalesce(supplierid,'')) = '' THEN NULL 
    ELSE trim(supplierid)::uuid 
  END
FROM products_staging
WHERE trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
ON CONFLICT (productid) DO NOTHING;
COMMIT;

-- Verification counts
SELECT
  (SELECT count(*) FROM products) AS products_in_final,
  (SELECT count(*) FROM products_staging) AS products_in_staging,
  (SELECT count(*) FROM products_problem_rows) AS products_problem_rows_count,
  (SELECT count(*) FROM suppliers) AS suppliers_now;

---------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Backup problematic customer rows
DROP TABLE IF EXISTS customers_problem_rows;
CREATE TABLE customers_problem_rows AS
SELECT *
FROM customers_staging
WHERE NOT (trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR (signupdate IS NOT NULL AND trim(signupdate) <> '' AND NOT (trim(signupdate) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'));

SELECT count(*) AS customers_problem_rows FROM customers_problem_rows;

-- Clean staging (trim)
UPDATE customers_staging
SET
  customerid = trim(customerid),
  name = trim(coalesce(name,'')),
  age = trim(coalesce(age,'')),
  gender = trim(coalesce(gender,'')),
  city = trim(coalesce(city,'')),
  state = trim(coalesce(state,'')),
  country = trim(coalesce(country,'')),
  signupdate = trim(coalesce(signupdate,'')),
  primemember = trim(coalesce(primemember,''));

-- Insert only valid UUID rows into customers
BEGIN;
INSERT INTO customers (customerid, name, age, gender, city, state, country, signupdate, primemember)
SELECT
  customerid::uuid,
  name,
  NULLIF(age,'')::int,
  NULLIF(gender,''),
  NULLIF(city,''),
  NULLIF(state,''),
  NULLIF(country,''),
  CASE WHEN signupdate ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
       THEN to_date(signupdate, 'MM/DD/YYYY')
       ELSE NULL
  END,
  CASE
    WHEN lower(primemember) IN ('yes','y','true','1') THEN true
    WHEN lower(primemember) IN ('no','n','false','0','') THEN false
    ELSE NULL
  END
FROM customers_staging
WHERE trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
ON CONFLICT (customerid) DO NOTHING;
COMMIT;

-- Final verification
SELECT
  (SELECT count(*) FROM customers) AS customers_in_final,
  (SELECT count(*) FROM customers_staging) AS customers_in_staging,
  (SELECT count(*) FROM customers_problem_rows) AS customers_problem_rows;


-------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Backup problematic rows (invalid UUIDs or bad dates)
DROP TABLE IF EXISTS orders_problem_rows;
CREATE TABLE orders_problem_rows AS
SELECT *
FROM orders_staging
WHERE NOT (trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR (
        trim(orderdate) <> '' AND NOT (trim(orderdate) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$')
      )
   OR (
        trim(shipdate) <> '' AND NOT (trim(shipdate) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$')
      );

SELECT count(*) AS orders_problem_rows FROM orders_problem_rows;

-- Clean staging
UPDATE orders_staging
SET
  orderid     = trim(orderid),
  customerid  = trim(customerid),
  orderdate   = trim(orderdate),
  shipdate    = trim(shipdate),
  shipmode    = trim(coalesce(shipmode, '')),
  totalamount = trim(coalesce(totalamount, ''));

-- Insert valid orders
BEGIN;
INSERT INTO orders (orderid, customerid, orderdate, shipdate, shipmode, totalamount)
SELECT
  orderid::uuid,
  customerid::uuid,
  CASE WHEN orderdate ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
       THEN to_date(orderdate, 'MM/DD/YYYY')
       ELSE NULL
  END,
  CASE WHEN shipdate ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
       THEN to_date(shipdate, 'MM/DD/YYYY')
       ELSE NULL
  END,
  shipmode,
  NULLIF(totalamount,'')::numeric
FROM orders_staging
WHERE
  trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND customerid IN (SELECT customerid FROM customers)
ON CONFLICT (orderid) DO NOTHING;
COMMIT;

-- Verification
SELECT
  (SELECT count(*) FROM orders) AS orders_in_final,
  (SELECT count(*) FROM orders_staging) AS orders_in_staging,
  (SELECT count(*) FROM orders_problem_rows) AS orders_problem_rows;

ROLLBACK;

--------------------------------------------------

DROP TABLE IF EXISTS orders_problem_rows;

CREATE TABLE orders_problem_rows AS
SELECT *
FROM orders_staging
WHERE NOT (trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR (
        trim(orderdate) <> '' AND NOT (trim(orderdate) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$')
      )
   OR (
        trim(shipdate) <> '' AND NOT (trim(shipdate) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$')
      );

SELECT count(*) AS orders_problem_rows FROM orders_problem_rows;

UPDATE orders_staging
SET
  orderid     = trim(orderid),
  customerid  = trim(customerid),
  orderdate   = trim(coalesce(orderdate,'')),
  shipdate    = trim(coalesce(shipdate,'')),
  shipmode    = trim(coalesce(shipmode,'')),
  totalamount = trim(coalesce(totalamount,''));


BEGIN;

INSERT INTO orders (orderid, customerid, orderdate, shipdate, shipmode, totalamount)
SELECT
  orderid::uuid,
  customerid::uuid,
  CASE WHEN orderdate ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
       THEN to_date(orderdate, 'MM/DD/YYYY')
       ELSE NULL
  END,
  CASE WHEN shipdate ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
       THEN to_date(shipdate, 'MM/DD/YYYY')
       ELSE NULL
  END,
  shipmode,
  NULLIF(totalamount,'')::numeric
FROM orders_staging
WHERE
  trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND customerid IN (SELECT customerid FROM customers)
ON CONFLICT (orderid) DO NOTHING;

COMMIT;


ROLLBACK;

-------------------------------------------------------------------

DROP TABLE IF EXISTS orders_problem_rows;

CREATE TABLE orders_problem_rows AS
SELECT *
FROM orders_staging
WHERE NOT (trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR (
        trim(orderdate) <> '' AND NOT (trim(orderdate) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$')
      )
   OR (
        trim(shipdate) <> '' AND NOT (trim(shipdate) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$')
      );

SELECT count(*) AS orders_problem_rows FROM orders_problem_rows;


SELECT DISTINCT customerid
FROM orders_staging
LIMIT 20;

SELECT DISTINCT orderid
FROM orders_staging
LIMIT 20;

ROLLBACK;

SELECT count(*) AS orders_with_missing_customer
FROM orders_staging os
LEFT JOIN customers c ON trim(os.customerid)::text = c.customerid::text
WHERE trim(os.customerid) <> '' AND c.customerid IS NULL;


BEGIN;

INSERT INTO orders (orderid, customerid, orderdate, shipdate, shipmode, totalamount)
SELECT
  trim(orderid)::uuid,
  trim(customerid)::uuid,
  CASE WHEN trim(orderdate) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
       THEN to_date(trim(orderdate), 'MM/DD/YYYY')
       ELSE NULL
  END,
  CASE WHEN trim(shipdate) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
       THEN to_date(trim(shipdate), 'MM/DD/YYYY')
       ELSE NULL
  END,
  NULLIF(trim(shipmode),''),
  NULLIF(trim(totalamount),'')::numeric
FROM orders_staging os
WHERE
  trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND trim(customerid) IN (SELECT customerid::text FROM customers)
ON CONFLICT (orderid) DO NOTHING;

COMMIT;

SELECT
  (SELECT count(*) FROM orders) AS orders_in_final,
  (SELECT count(*) FROM orders_staging) AS orders_in_staging,
  (SELECT count(*) FROM orders_problem_rows) AS orders_problem_rows;


---------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 1) backup bad order_details rows (non-UUID ids)
DROP TABLE IF EXISTS order_details_problem_rows;
CREATE TABLE order_details_problem_rows AS
SELECT *
FROM order_details_staging
WHERE NOT (trim(orderdetailid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

SELECT count(*) AS backed_up_problem_rows FROM order_details_problem_rows;


CREATE EXTENSION IF NOT EXISTS pgcrypto;

UPDATE order_details_staging
SET orderdetailid = gen_random_uuid()::text
WHERE NOT (trim(orderdetailid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');
-- verify none left
SELECT count(*) AS still_bad_orderdetailid
FROM order_details_staging
WHERE NOT (trim(orderdetailid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');


DELETE FROM order_details_staging
WHERE NOT (trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

SELECT count(*) AS staging_remaining FROM order_details_staging;
SELECT count(*) AS problem_rows_saved FROM order_details_problem_rows;

--------------------------------------------------------------------------------------------------------------------

SELECT count(*) AS problem_count FROM order_details_problem_rows;
SELECT count(DISTINCT trim(orderid)) AS distinct_bad_orderids FROM order_details_problem_rows;
SELECT count(DISTINCT trim(productid)) AS distinct_bad_productids FROM order_details_problem_rows;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- mapping for orderids
DROP TABLE IF EXISTS mapping_orderids;
CREATE TABLE mapping_orderids AS
SELECT DISTINCT trim(orderid) AS old_orderid_text,
       gen_random_uuid() AS new_orderid_uuid
FROM order_details_problem_rows
WHERE trim(orderid) <> '';

-- mapping for productids
DROP TABLE IF EXISTS mapping_productids;
CREATE TABLE mapping_productids AS
SELECT DISTINCT trim(productid) AS old_productid_text,
       gen_random_uuid() AS new_productid_uuid
FROM order_details_problem_rows
WHERE trim(productid) <> '';

-- quick checks
SELECT count(*) AS orderid_map_count FROM mapping_orderids;
SELECT count(*) AS productid_map_count FROM mapping_productids;



-- Insert placeholder ORDERS for each mapped orderid
INSERT INTO orders (orderid, customerid, orderdate, shipdate, shipmode, totalamount)
SELECT new_orderid_uuid, NULL::uuid, NULL::date, NULL::date, 'PLACEHOLDER', NULL::numeric
FROM mapping_orderids
ON CONFLICT (orderid) DO NOTHING;

-- Insert placeholder PRODUCTS for each mapped productid
INSERT INTO products (productid, productname, category, subcategory, priceperunit, stockquantity, supplierid)
SELECT new_productid_uuid, 'UNKNOWN PRODUCT', NULL, NULL, NULL::numeric, NULL::int, NULL::uuid
FROM mapping_productids
ON CONFLICT (productid) DO NOTHING;

-- verify
SELECT (SELECT count(*) FROM orders) AS total_orders,
       (SELECT count(*) FROM products) AS total_products,
       (SELECT count(*) FROM mapping_orderids) AS mapping_order_count,
       (SELECT count(*) FROM mapping_productids) AS mapping_product_count;


-- update orderid in problem rows
UPDATE order_details_problem_rows p
SET orderid = m.new_orderid_uuid::text
FROM mapping_orderids m
WHERE trim(p.orderid) = m.old_orderid_text;

-- update productid in problem rows
UPDATE order_details_problem_rows p
SET productid = m.new_productid_uuid::text
FROM mapping_productids m
WHERE trim(p.productid) = m.old_productid_text;

-- verify replacements
SELECT
  (SELECT count(*) FROM order_details_problem_rows WHERE NOT (trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')) AS still_bad_orderid_count,
  (SELECT count(*) FROM order_details_problem_rows WHERE NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')) AS still_bad_productid_count;



UPDATE order_details_problem_rows
SET orderdetailid = gen_random_uuid()::text
WHERE NOT (trim(orderdetailid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

SELECT count(*) AS still_bad_orderdetailid
FROM order_details_problem_rows
WHERE NOT (trim(orderdetailid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');



-------------------------------------------------------------------------------------------------------

BEGIN;

INSERT INTO order_details (orderdetailid, orderid, productid, quantity, unitprice, discount)
SELECT
  trim(orderdetailid)::uuid,
  trim(orderid)::uuid,
  trim(productid)::uuid,
  NULLIF(trim(quantity),'')::int,
  NULLIF(trim(unitprice),'')::numeric,
  NULLIF(trim(discount),'')::numeric
FROM order_details_problem_rows
WHERE orderdetailid IS NOT NULL AND trim(orderdetailid) <> ''
  -- ensure parents exist (defensive)
  AND trim(orderid) IN (SELECT orderid::text FROM orders)
  AND trim(productid) IN (SELECT productid::text FROM products)
ON CONFLICT (orderdetailid) DO NOTHING;

COMMIT;

-- verification
SELECT
  (SELECT count(*) FROM order_details)          AS order_details_in_final,
  (SELECT count(*) FROM order_details_problem_rows) AS order_details_problem_rows_remaining,
  (SELECT count(*) FROM order_details_staging)  AS order_details_staging_remaining;


  -- After manual review, if you want to drop helper/mapping tables:
DROP TABLE IF EXISTS mapping_orderids;
DROP TABLE IF EXISTS mapping_productids;
DROP TABLE IF EXISTS order_details_problem_rows;
DROP TABLE IF EXISTS order_details_staging;


ROLLBACK;

-- how many distinct order/product ids remain in staging
SELECT
  (SELECT count(DISTINCT trim(orderid)) FROM order_details_staging) AS distinct_orderids_in_staging,
  (SELECT count(DISTINCT trim(productid)) FROM order_details_staging) AS distinct_productids_in_staging,
  (SELECT count(*) FROM order_details_problem_rows) AS problem_rows_saved;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- create orders placeholders for UUID orderids referenced in staging but missing in orders
INSERT INTO orders (orderid, customerid, orderdate, shipdate, shipmode, totalamount)
SELECT DISTINCT trim(ods.orderid)::uuid, NULL::uuid, NULL::date, NULL::date, 'PLACEHOLDER', NULL::numeric
FROM order_details_staging ods
LEFT JOIN orders o ON trim(ods.orderid)::text = o.orderid::text
WHERE trim(ods.orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND ods.orderid IS NOT NULL
  AND o.orderid IS NULL
ON CONFLICT (orderid) DO NOTHING;

-- create product placeholders for UUID productids referenced in staging but missing in products
INSERT INTO products (productid, productname, category, subcategory, priceperunit, stockquantity, supplierid)
SELECT DISTINCT trim(ods.productid)::uuid, 'UNKNOWN PRODUCT', NULL, NULL, NULL::numeric, NULL::int, NULL::uuid
FROM order_details_staging ods
LEFT JOIN products p ON trim(ods.productid)::text = p.productid::text
WHERE trim(ods.productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND ods.productid IS NOT NULL
  AND p.productid IS NULL
ON CONFLICT (productid) DO NOTHING;

-- quick verify
SELECT
  (SELECT count(*) FROM orders) AS total_orders,
  (SELECT count(*) FROM products) AS total_products;


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- ensure every staging row has a UUID PK
UPDATE order_details_staging
SET orderdetailid = gen_random_uuid()::text
WHERE NOT (trim(orderdetailid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

-- insert cleaned staging rows whose parents now exist
BEGIN;
INSERT INTO order_details (orderdetailid, orderid, productid, quantity, unitprice, discount)
SELECT
  trim(orderdetailid)::uuid,
  trim(orderid)::uuid,
  trim(productid)::uuid,
  NULLIF(trim(quantity),'')::int,
  NULLIF(trim(unitprice),'')::numeric,
  NULLIF(trim(discount),'')::numeric
FROM order_details_staging
WHERE orderdetailid IS NOT NULL AND trim(orderdetailid) <> ''
  -- only insert rows where parents exist (defensive)
  AND trim(orderid) IN (SELECT orderid::text FROM orders)
  AND trim(productid) IN (SELECT productid::text FROM products)
ON CONFLICT (orderdetailid) DO NOTHING;
COMMIT;

-- results
SELECT
  (SELECT count(*) FROM order_details) AS order_details_now,
  (SELECT count(*) FROM order_details_staging) AS remaining_in_staging,
  (SELECT count(*) FROM order_details_problem_rows) AS problem_rows_saved;


  ------------------------------------------------------------------------------------------------------------------------------------------------------

  -- create mapping tables for any remaining non-UUID order/product ids in problem rows
DROP TABLE IF EXISTS mapping_orderids;
CREATE TABLE mapping_orderids AS
SELECT DISTINCT trim(orderid) AS old_orderid_text,
       gen_random_uuid() AS new_orderid_uuid
FROM order_details_problem_rows
WHERE trim(orderid) <> '' AND NOT (trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

DROP TABLE IF EXISTS mapping_productids;
CREATE TABLE mapping_productids AS
SELECT DISTINCT trim(productid) AS old_productid_text,
       gen_random_uuid() AS new_productid_uuid
FROM order_details_problem_rows
WHERE trim(productid) <> '' AND NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

-- insert placeholders using these new UUIDs
INSERT INTO orders (orderid, customerid, orderdate, shipdate, shipmode, totalamount)
SELECT new_orderid_uuid, NULL::uuid, NULL::date, NULL::date, 'PLACEHOLDER', NULL::numeric
FROM mapping_orderids
ON CONFLICT (orderid) DO NOTHING;

INSERT INTO products (productid, productname, category, subcategory, priceperunit, stockquantity, supplierid)
SELECT new_productid_uuid, 'UNKNOWN PRODUCT', NULL, NULL, NULL::numeric, NULL::int, NULL::uuid
FROM mapping_productids
ON CONFLICT (productid) DO NOTHING;

-- quick verify mapping sizes
SELECT (SELECT count(*) FROM mapping_orderids) AS mapped_orders,
       (SELECT count(*) FROM mapping_productids) AS mapped_products;


-- replace orderids in the problem rows
UPDATE order_details_problem_rows p
SET orderid = m.new_orderid_uuid::text
FROM mapping_orderids m
WHERE trim(p.orderid) = m.old_orderid_text;

-- replace productids in the problem rows
UPDATE order_details_problem_rows p
SET productid = m.new_productid_uuid::text
FROM mapping_productids m
WHERE trim(p.productid) = m.old_productid_text;

-- ensure PKs are valid UUIDs
UPDATE order_details_problem_rows
SET orderdetailid = gen_random_uuid()::text
WHERE NOT (trim(orderdetailid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

-- verify none remain invalid
SELECT
  (SELECT count(*) FROM order_details_problem_rows WHERE NOT (trim(orderid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')) AS still_bad_orderid,
  (SELECT count(*) FROM order_details_problem_rows WHERE NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')) AS still_bad_productid,
  (SELECT count(*) FROM order_details_problem_rows WHERE NOT (trim(orderdetailid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')) AS still_bad_orderdetailid;




BEGIN;

INSERT INTO order_details (orderdetailid, orderid, productid, quantity, unitprice, discount)
SELECT
  trim(orderdetailid)::uuid,
  trim(orderid)::uuid,
  trim(productid)::uuid,
  NULLIF(trim(quantity),'')::int,
  NULLIF(trim(unitprice),'')::numeric,
  NULLIF(trim(discount),'')::numeric
FROM order_details_problem_rows
WHERE orderdetailid IS NOT NULL AND trim(orderdetailid) <> ''
  AND trim(orderid) IN (SELECT orderid::text FROM orders)
  AND trim(productid) IN (SELECT productid::text FROM products)
ON CONFLICT (orderdetailid) DO NOTHING;

COMMIT;

-- final verification
SELECT
  (SELECT count(*) FROM order_details) AS order_details_final_count,
  (SELECT count(*) FROM order_details_staging) AS order_details_staging_remaining,
  (SELECT count(*) FROM order_details_problem_rows) AS order_details_problem_rows_remaining;

DROP TABLE IF EXISTS order_details_staging;
DROP TABLE IF EXISTS order_details_problem_rows;
DROP TABLE IF EXISTS mapping_orderids;
DROP TABLE IF EXISTS mapping_productids;


-------------------------------------------------------------------------------------------------------------------------------------


ROLLBACK;


SELECT count(*) FROM reviews_staging;


DROP TABLE IF EXISTS reviews_problem_rows;

CREATE TABLE reviews_problem_rows AS
SELECT *
FROM reviews_staging
WHERE NOT (trim(reviewid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR (trim(customerid) <> '' AND NOT (trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'));

SELECT count(*) AS backed_up_review_rows FROM reviews_problem_rows;



CREATE EXTENSION IF NOT EXISTS pgcrypto;



UPDATE reviews_staging
SET reviewid = gen_random_uuid()::text
WHERE NOT (trim(reviewid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

SELECT count(*) AS still_bad_reviewid
FROM reviews_staging
WHERE NOT (trim(reviewid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

-----------------------------------------------------------------------------

DELETE FROM reviews_staging
WHERE NOT (trim(productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
   OR (trim(customerid) <> '' AND NOT (trim(customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'));

SELECT count(*) AS reviews_staging_remaining FROM reviews_staging;
SELECT count(*) AS reviews_problem_rows_saved FROM reviews_problem_rows;



--------------------------------------

INSERT INTO products (productid, productname)
SELECT DISTINCT trim(rs.productid)::uuid, 'UNKNOWN PRODUCT'
FROM reviews_staging rs
LEFT JOIN products p ON trim(rs.productid)::text = p.productid::text
WHERE trim(rs.productid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND p.productid IS NULL
ON CONFLICT (productid) DO NOTHING;

INSERT INTO customers (customerid, name)
SELECT DISTINCT trim(rs.customerid)::uuid, 'UNKNOWN CUSTOMER'
FROM reviews_staging rs
LEFT JOIN customers c ON trim(rs.customerid)::text = c.customerid::text
WHERE trim(rs.customerid) <> '' 
  AND trim(rs.customerid) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND c.customerid IS NULL
ON CONFLICT (customerid) DO NOTHING;

-- quick verify counts
SELECT (SELECT count(*) FROM products) AS total_products,
       (SELECT count(*) FROM customers) AS total_customers;


---------------------------------------------------------------------------------------

BEGIN;

INSERT INTO reviews (reviewid, productid, customerid, rating, reviewtext)
SELECT
  trim(reviewid)::uuid,
  trim(productid)::uuid,
  CASE WHEN trim(customerid) = '' THEN NULL ELSE trim(customerid)::uuid END,
  NULLIF(trim(rating),'')::int,
  reviewtext
FROM reviews_staging
WHERE reviewid IS NOT NULL AND trim(reviewid) <> ''
  AND trim(productid) IN (SELECT productid::text FROM products)
  AND (trim(customerid) = '' OR trim(customerid) IN (SELECT customerid::text FROM customers))
ON CONFLICT (reviewid) DO NOTHING;

COMMIT;

-- verification
SELECT
  (SELECT count(*) FROM reviews) AS reviews_in_final,
  (SELECT count(*) FROM reviews_staging) AS reviews_staging_remaining,
  (SELECT count(*) FROM reviews_problem_rows) AS reviews_problem_rows_remaining;


ROLLBACK;

SELECT 
  (SELECT count(*) FROM reviews_staging)                     AS staging_total,
  (SELECT count(*) FROM reviews WHERE reviewid IS NOT NULL) AS final_total,
  (SELECT count(*) 
     FROM reviews_staging rs
     JOIN reviews r ON trim(rs.reviewid)::text = r.reviewid::text
  )                                                         AS duplicates_between_staging_and_final,
  (SELECT count(*) 
     FROM reviews_staging rs
     LEFT JOIN reviews r ON trim(rs.reviewid)::text = r.reviewid::text
     WHERE r.reviewid IS NULL
  )                                                         AS staging_not_in_final;





-- verify final count one more time (optional)
SELECT count(*) AS reviews_final_count FROM reviews;

-- drop staging + problem helper tables (safe)
DROP TABLE IF EXISTS reviews_staging;
DROP TABLE IF EXISTS reviews_problem_rows;
DROP TABLE IF EXISTS mapping_reviews_productids;
DROP TABLE IF EXISTS mapping_reviews_customerids;

-- optional: reclaim space / refresh planner statistics
VACUUM ANALYZE reviews;



------------------------------------------------------------------------------------------------------------



SELECT
  (SELECT count(*) FROM customers)      AS customers,
  (SELECT count(*) FROM orders)         AS orders,
  (SELECT count(*) FROM products)       AS products,
  (SELECT count(*) FROM suppliers)      AS suppliers,
  (SELECT count(*) FROM order_details)  AS order_details,
  (SELECT count(*) FROM reviews)        AS reviews;


SELECT
  (SELECT count(*) FROM customers) - (SELECT count(DISTINCT customerid) FROM customers) AS customers_duplicate_pks,
  (SELECT count(*) FROM orders)    - (SELECT count(DISTINCT orderid)    FROM orders)    AS orders_duplicate_pks,
  (SELECT count(*) FROM products)  - (SELECT count(DISTINCT productid)  FROM products)  AS products_duplicate_pks,
  (SELECT count(*) FROM suppliers) - (SELECT count(DISTINCT supplierid) FROM suppliers) AS suppliers_duplicate_pks,
  (SELECT count(*) FROM order_details) - (SELECT count(DISTINCT orderdetailid) FROM order_details) AS order_details_duplicate_pks,
  (SELECT count(*) FROM reviews) - (SELECT count(DISTINCT reviewid) FROM reviews) AS reviews_duplicate_pks;


-- order_details -> orders | products
SELECT
  (SELECT count(*) FROM order_details od LEFT JOIN orders o ON od.orderid = o.orderid WHERE o.orderid IS NULL) AS od_orphan_orders,
  (SELECT count(*) FROM order_details od LEFT JOIN products p ON od.productid = p.productid WHERE p.productid IS NULL) AS od_orphan_products;

-- orders -> customers
SELECT count(*) AS orders_orphan_customers
FROM orders o LEFT JOIN customers c ON o.customerid = c.customerid
WHERE o.customerid IS NOT NULL AND c.customerid IS NULL;

-- products -> suppliers
SELECT count(*) AS products_orphan_suppliers
FROM products p LEFT JOIN suppliers s ON p.supplierid = s.supplierid
WHERE p.supplierid IS NOT NULL AND s.supplierid IS NULL;

-- reviews -> products | customers (customer can be NULL)
SELECT
  (SELECT count(*) FROM reviews r LEFT JOIN products p ON r.productid = p.productid WHERE p.productid IS NULL) AS reviews_orphan_products,
  (SELECT count(*) FROM reviews r LEFT JOIN customers c ON r.customerid = c.customerid WHERE r.customerid IS NOT NULL AND c.customerid IS NULL) AS reviews_orphan_customers;



SELECT
  (SELECT count(*) FROM customers WHERE customerid IS NULL) AS customers_null_pk,
  (SELECT count(*) FROM orders WHERE orderid IS NULL) AS orders_null_pk,
  (SELECT count(*) FROM products WHERE productid IS NULL) AS products_null_pk,
  (SELECT count(*) FROM suppliers WHERE supplierid IS NULL) AS suppliers_null_pk,
  (SELECT count(*) FROM order_details WHERE orderdetailid IS NULL) AS od_null_pk,
  (SELECT count(*) FROM reviews WHERE reviewid IS NULL) AS reviews_null_pk;


SELECT * FROM customers LIMIT 5;
SELECT * FROM orders ORDER BY orderdate DESC NULLS LAST LIMIT 5;
SELECT * FROM products LIMIT 5;
SELECT * FROM order_details LIMIT 5;
SELECT * FROM reviews LIMIT 5;

