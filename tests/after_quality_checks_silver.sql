-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No Result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname);


-- Data Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;


-- /-------------------------------------------------------------
-- Quality Checks
-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- Check for unwanted Spaces
-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;


-- Check for Invalid Date Orders
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- /------------------------------------------------------------
-- Quality Checks

-- Checking for the length of the date values
-- When with start and end dates, or any, check for the dates lies in the boundry
-- Eg. sls_order_dt > 19000101 OR sls_order_dt <= 20501231
-- Check for each date column
SELECT sls_ord_num,
sls_order_dt
FROM silver.crm_sales_details
GROUP BY sls_order_dt, sls_ord_num
HAVING LENGTH(CAST(sls_order_dt AS VARCHAR)) < 6;


-- order date must be smaller than ship date, 
-- and ship date, order date must be less than due date
-- Check for all date columns
SELECT * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


-- Sales = Quantity * Price
-- Negatives, Zeros, Nulls are not allowed
SELECT * FROM silver.crm_sales_details WHERE sls_sales <= 0 OR sls_sales IS NULL;

SELECT * FROM silver.crm_sales_details WHERE sls_quantity <= 0 OR sls_quantity IS NULL;

SELECT * FROM silver.crm_sales_details WHERE sls_price <= 0 OR sls_price IS NULL;



-- // There is no issues with quantity column //
-- if Sales is negative, zero, or null derive it using quantity and price
-- if price is zero or null, calculate it using sales and quantity
-- if price is negative, convert it to a postitve value
SELECT DISTINCT sls_sales, sls_quantity, sls_price FROM silver.crm_sales_details 
WHERE sls_sales <> sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL;


-- /-------------------------------------------------------------------------
-- Data cleaning
SELECT cid, bdate, gen
FROM silver.erp_cust_az12;

-- to connect this table to crm_cust_info using cid and cust_key
-- cid has 'NAS' Extra so need to be removed

SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE cid
END cid,
bdate,
gen
FROM silver.erp_cust_az12;

-- trying to join tables for if any not matching values in join
-- Expecations: No Result
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE cid
END cid,
bdate,
gen
FROM silver.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);


-- Lets say business says the bdate must be from 1925-01-01 to NOW()
-- List of invalid dates
SELECT DISTINCT
bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1925-01-01' OR bdate > NOW();

-- Data Standardization and Consistency
-- Errors available
SELECT DISTINCT gen FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;


-- /------------------------------------------------------------------
SELECT * FROM silver.erp_loc_a101;

-- No Nulls available
SELECT cid FROM silver.erp_loc_a101 WHERE cid IS NULL;

-- difference in cid and cst_key (cid has a '-')
-- transformed and no unmatched rows
SELECT 
REPLACE(cid, '-', '') AS cid 
FROM silver.erp_loc_a101 
WHERE REPLACE(cid, '-', '')
NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- checking cntry
-- erros available
SELECT DISTINCT cntry FROM silver.erp_loc_a101 ORDER BY cntry;

-- /-----------------------------------------------------------------
SELECT * FROM silver.erp_px_cat_g1v2;

-- check for unwanted spaces
-- clean already
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat);

-- similarly for subcat and maintenance

-- Data standardization & consistency
-- everything is ok.






