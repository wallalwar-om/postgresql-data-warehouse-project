/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL Silver.load_silver;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	elapsed INTERVAL;
BEGIN

	RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '==============================';


    -- crm_cust_info
    start_time := clock_timestamp();
	TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info(
		cst_id, 
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status, 
		cst_gndr,
		cst_create_date)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date
		FROM (SELECT *,
		ROW_NUMBER() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL) temp
	WHERE flag_last = 1;
	end_time := clock_timestamp();
    elapsed := end_time - start_time;
    RAISE NOTICE 'Loaded crm_cust_info in % seconds', EXTRACT(SECOND FROM elapsed);


	-- crm_prd_info
    start_time := clock_timestamp();
	TRUNCATE TABLE silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
	SELECT 
		prd_id, 
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
		prd_nm,
		COALESCE(prd_cost, 0) AS prd_cost, -- If the value is NULL, then write zero
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
	FROM bronze.crm_prd_info;
	end_time := clock_timestamp();
    elapsed := end_time - start_time;
    RAISE NOTICE 'Loaded crm_prd_info in % seconds', EXTRACT(SECOND FROM elapsed);


    -- crm_sales_details
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.crm_sales_details;
	INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN LENGTH(CAST(sls_order_dt AS TEXT)) < 8 THEN NULL
			ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
		END AS sls_order_dt,
		CASE WHEN LENGTH(CAST(sls_ship_dt AS TEXT)) < 8 THEN NULL
			ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
		END AS sls_ship_dt,
		CASE WHEN LENGTH(CAST(sls_due_dt AS TEXT)) < 8 THEN NULL
			ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details;
	end_time := clock_timestamp();
    elapsed := end_time - start_time;
    RAISE NOTICE 'Loaded crm_sales_details in % seconds', EXTRACT(SECOND FROM elapsed);


    -- erp_cust_az12
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.erp_cust_az12;
	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
	SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END cid,
		CASE WHEN bdate > NOW() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
	FROM bronze.erp_cust_az12;
	end_time := clock_timestamp();
    elapsed := end_time - start_time;
    RAISE NOTICE 'Loaded erp_cust_az12 in % seconds', EXTRACT(SECOND FROM elapsed);


	-- erp_loc_a101
	start_time := clock_timestamp();
	TRUNCATE TABLE silver.erp_loc_a101;
	INSERT INTO silver.erp_loc_a101 (cid, cntry)
	SELECT 
		REPLACE(cid, '-', '') AS cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
	FROM bronze.erp_loc_a101;
	end_time := clock_timestamp();
    elapsed := end_time - start_time;
    RAISE NOTICE 'Loaded erp_loc_a101 in % seconds', EXTRACT(SECOND FROM elapsed);


	-- erp_px_cat_g1v2
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	end_time := clock_timestamp();
    elapsed := end_time - start_time;
    RAISE NOTICE 'Loaded erp_px_cat_g1v2 in % seconds', EXTRACT(SECOND FROM elapsed);

EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE '================================================';
		RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==========================================';
END;
$$;



-- /-------------------------------------------------------------------------------------------------------------/
-- Truncate table if exists using procedure
-- /----------------------------------------/

-- CREATE OR REPLACE FUNCTION truncate_if_exists(tablename text) RETURNS void LANGUAGE plpgsql AS $$
-- BEGIN
--     SELECT FROM information_schema.tables WHERE table_name = tablename;
--     IF FOUND THEN
--         EXECUTE format('TRUNCATE TABLE %I', tablename);
--     END IF;
-- END;
-- $$;