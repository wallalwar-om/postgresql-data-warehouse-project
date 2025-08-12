/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
);


DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
prd_id INT,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
);


DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);


DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
cid VARCHAR(50),
cntry VARCHAR(50)
);


DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50)
);


DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2(
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50)
);


CALL bronze.load_bronze();


-- SELECT * FROM bronze.crm_prd_info LIMIT 50;
-- SELECT * FROM bronze.crm_cust_info LIMIT 50;
-- SELECT * FROM bronze.crm_sales_details LIMIT 50;
-- SELECT * FROM bronze.erp_loc_a101 LIMIT 50;
-- SELECT * FROM bronze.erp_cust_az12 LIMIT 50;
-- SELECT * FROM bronze.erp_px_cat_g1v2 LIMIT 50;







