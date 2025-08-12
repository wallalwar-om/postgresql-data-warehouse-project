/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- ===== Customers =======
-- Collected all the customers information that is available
-- By joining all the related tables

-- SELECT 
-- 	ci.cst_id, 
-- 	ci.cst_key,
-- 	ci.cst_firstname,
-- 	ci.cst_lastname,
-- 	ci.cst_marital_status,
-- 	ci.cst_gndr,
-- 	ci.cst_create_date,
-- 	ca.bdate,
-- 	ca.gen,
-- 	la.cntry
-- FROM silver.crm_cust_info ci
-- LEFT JOIN silver.erp_cust_az12 ca
-- 	ON ci.cst_key = ca.cid
-- LEFT JOIN silver.erp_loc_a101 la
-- 	ON ci.cst_key = la.cid;


-- **TIP - After Joining table, check if any duplicates were introducted by
--         join logic
-- Expectation: No Duplicates

-- Checking For Duplicates

-- SELECT cst_id, COUNT(*) FROM (
-- 	SELECT ci.cst_id, ci.cst_key, ci.cst_firstname, ci.cst_lastname,
-- 	ci.cst_marital_status, ci.cst_gndr, ci.cst_create_date, ca.bdate,
-- 	ca.gen, la.cntry
-- 	FROM silver.crm_cust_info ci
-- 	LEFT JOIN silver.erp_cust_az12 ca
-- 	ON ci.cst_key = ca.cid
-- 	LEFT JOIN silver.erp_loc_a101 la
-- 	ON ci.cst_key = la.cid
-- )
-- GROUP BY cst_id
-- HAVING COUNT(*) > 1
-- ORDER BY cst_id DESC;


-- Here we can see that there are two gender columns
-- So let's do data integration with those two

-- SELECT 
-- 	ci.cst_id AS customer_id,
-- 	ci.cst_key AS customer_number,  
-- 	ci.cst_firstname AS first_name, 
-- 	ci.cst_lastname AS last_name,
-- 	la.cntry AS country,
-- 	ci.cst_marital_status AS marital_status, 
-- 	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is Master for gender info
-- 		ELSE COALESCE(ca.gen, 'n/a')
-- 	END AS gender,
-- 	ci.cst_create_date AS create_date, 
-- 	ca.bdate AS birthdate
-- FROM silver.crm_cust_info ci
-- LEFT JOIN silver.erp_cust_az12 ca
-- 	ON ci.cst_key = ca.cid
-- LEFT JOIN silver.erp_loc_a101 la
-- 	ON ci.cst_key = la.cid;

-- Dimension or Fact
-- Dimensions are the descriptive information
-- We have information about the customers here so it is dimension

-- Surrogate Key: System-generated unique identifier assigned to
-- 				  each record in a table in warehouse.
-- It can be generated:
-- 1. DDL-based generation.
-- 2. Query-based using Window function (Row_Number)


-- Last step: Create Object and as decided all the objects in gold layer
--            are virtual
-- So, Creating view in gold layer

-- =================================
-- Droping if exists.

DROP VIEW IF EXISTS gold.fact_sales;
DROP VIEW IF EXISTS gold.dim_customers;
DROP VIEW IF EXISTS gold.dim_products;

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate Key
	ci.cst_id 				AS customer_id,
	ci.cst_key 				AS customer_number,  
	ci.cst_firstname 		AS first_name, 
	ci.cst_lastname 		AS last_name,
	la.cntry 				AS country,
	ci.cst_marital_status 	AS marital_status, 
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is Master for gender info
		ELSE COALESCE(ca.gen, 'n/a')
	END 					AS gender,
	ci.cst_create_date 		AS create_date, 
	ca.bdate 				AS birthdate
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid;


-- /----------------------------------------------------------
-- ===== Products =======

-- Current Products
-- **TIP - After Joining table, check if any duplicates were introducted by
--         join logic
-- Expectation: No Duplicates

-- Dimension or Fact
-- *Dimension here as it has information related to products*

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
-- SELECT prd_key, COUNT(*) FROM (

CREATE VIEW gold.dim_products AS
SELECT 
 	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate Key
	pn.prd_id 			AS product_id,
	pn.prd_key 			AS product_number,
	pn.prd_nm 			AS product_name,
	pn.cat_id 			AS category_id,
	pc.cat 				AS category,
	pc.subcat 			AS suncategory,  
	pc.maintenance 		AS maintenance,
	pn.prd_cost 		AS cost,
	pn.prd_line 		AS product_line,
	pn.prd_start_dt 	AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL; -- current data, no historical
-- )
-- GROUP BY prd_key
-- HAVING COUNT(*) > 1;


-- /---------------------------------------------------------------
-- ===== Sales =======

-- Dimension vs Fact
-- It is a Fact, it has keys connecting multiple dimensions, dates, measures

-- Here now in sales table we are using prd_key and cust_id to connect
-- but for customers and product now we have surrogate keys,
-- So, lets replace this columns with surrogate keys columns from both
-- This process is called data lookup


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

CREATE VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num 		AS order_number,
pr.product_key,		-- surroegate key from products (dimension key)
cu.customer_key, 	-- surrogate key from customers (dimension key)
sd.sls_order_dt 	AS order_date,
sd.sls_ship_dt 		AS shipping_date,
sd.sls_due_dt 		AS due_date,
sd.sls_sales 		AS sales_amount,
sd.sls_quantity 	AS quantity,
sd.sls_price 		AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;


-- Data validation after building gold views
SELECT COUNT(*) FROM gold.dim_customers;
SELECT COUNT(*) FROM gold.dim_products;
SELECT COUNT(*) FROM gold.fact_sales;



