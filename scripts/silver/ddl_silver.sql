/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE IF NOT EXISTS silver.crm_cust_info (
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE,
dwh_create_date TIMESTAMPTZ DEFAULT NOW() 
);


DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE IF NOT EXISTS silver.crm_prd_info (
prd_id INT,
cat_id VARCHAR(50),
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date TIMESTAMPTZ DEFAULT NOW() 
);


DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE IF NOT EXISTS silver.crm_sales_details (
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date TIMESTAMPTZ DEFAULT NOW()
);


DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE IF NOT EXISTS silver.erp_loc_a101 (
cid VARCHAR(50),
cntry VARCHAR(50),
dwh_create_date TIMESTAMPTZ DEFAULT NOW()
);


DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE IF NOT EXISTS silver.erp_cust_az12 (
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50),
dwh_create_date TIMESTAMPTZ DEFAULT NOW()
);


DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2(
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50),
dwh_create_date TIMESTAMPTZ DEFAULT NOW()
);


CALL silver.load_silver();

-- SELECT * FROM silver.crm_cust_info LIMIT 50;
-- SELECT * FROM silver.crm_prd_info LIMIT 50;
-- SELECT * FROM silver.crm_sales_details LIMIT 50;
-- SELECT * FROM silver.erp_loc_a101 LIMIT 50;
-- SELECT * FROM silver.erp_cust_az12 LIMIT 50;
-- SELECT * FROM silver.erp_px_cat_g1v2 LIMIT 50;







