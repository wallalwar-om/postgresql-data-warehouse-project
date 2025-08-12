/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY ... FROM ... WITH` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze;
===============================================================================
*/


-- COPY table_name (column1, column2, ...) FROM '/path/to/data.csv' WITH (FORMAT CSV, HEADER);
-- COPY table_name FROM STDIN;

-- Converting to Procedure
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	elapsed INTERVAL;
BEGIN
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '-------------------------';

	--------------------------------------------------------------------
    -- CRM Tables
    --------------------------------------------------------------------
	
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '-------------------------';

	-- crm_cust_info
	start_time := clock_timestamp();

	TRUNCATE TABLE bronze.crm_cust_info;
	COPY bronze.crm_cust_info 
	(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date) 
	FROM 'C:\Users\HP\Desktop\postgresql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE);

	end_time := clock_timestamp();
	elapsed := end_time - start_time;
	RAISE NOTICE 'Loaded crm_cust_info in % seconds', EXTRACT(SECOND FROM elapsed);
	
	
	-- crm_prd_info
	start_time := clock_timestamp();

	TRUNCATE TABLE bronze.crm_prd_info;
	COPY bronze.crm_prd_info 
	(prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt) 
	FROM 'C:\Users\HP\Desktop\postgresql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE);
	
	end_time := clock_timestamp();
    elapsed := end_time - start_time;
    RAISE NOTICE 'Loaded crm_prd_info in % seconds', EXTRACT(SECOND FROM elapsed);


    -- crm_sales_details
	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.crm_sales_details;
	COPY bronze.crm_sales_details 
	(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price) 
	FROM 'C:\Users\HP\Desktop\postgresql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE);
	end_time := clock_timestamp();
    elapsed := end_time - start_time;
    RAISE NOTICE 'Loaded crm_sales_details in % seconds', EXTRACT(SECOND FROM elapsed);


	--------------------------------------------------------------------
    -- ERP Tables
    --------------------------------------------------------------------
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------';
    
    -- erp_loc_a101
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    (cid, cntry) 
    FROM 'C:\Users\HP\Desktop\postgresql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE);
    end_time := clock_timestamp();
    elapsed := end_time - start_time;
    RAISE NOTICE 'Loaded erp_loc_a101 in % seconds', EXTRACT(SECOND FROM elapsed);

    -- erp_cust_az12
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    (cid, bdate, gen) 
    FROM 'C:\Users\HP\Desktop\postgresql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE);
    end_time := clock_timestamp();
    elapsed := end_time - start_time;
    RAISE NOTICE 'Loaded erp_cust_az12 in % seconds', EXTRACT(SECOND FROM elapsed);

    -- erp_px_cat_g1v2
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    (id, cat, subcat, maintenance) 
    FROM 'C:\Users\HP\Desktop\postgresql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE);
    end_time := clock_timestamp();
    elapsed := end_time - start_time;
    RAISE NOTICE 'Loaded erp_px_cat_g1v2 in % seconds', EXTRACT(SECOND FROM elapsed);

    RAISE NOTICE '==============================';
    RAISE NOTICE 'Bronze Layer Load Completed Successfully';
    RAISE NOTICE '==============================';
	
EXCEPTION 
	WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==========================================';
END;
$$;

-- /----------------------------------------------------------------------------------------------------------------------------------------------/
-- Truncate table if table has columns
-- /------------------------------------/

-- %I when only table name in place of %s

-- CREATE OR REPLACE FUNCTION truncate_if_not_empty(table_name_text TEXT)
-- RETURNS VOID AS $$
-- DECLARE
--     row_count INTEGER;
-- BEGIN
--     EXECUTE format('SELECT count(*) FROM %s', table_name_text) INTO row_count;

--     IF row_count > 0 THEN
--         EXECUTE format('TRUNCATE TABLE %s', table_name_text);
--         RAISE NOTICE 'Table % truncated successfully.', table_name_text;
--     ELSE
--         RAISE NOTICE 'Table % is already empty, no truncation performed.', table_name_text;
--     END IF;
-- END;
-- $$ LANGUAGE plpgsql;

-- SELECT truncate_if_not_empty('bronze.crm_cust_info');


-- DO $$
-- DECLARE
--     tbl TEXT;
--     table_list TEXT[] := ARRAY[
--         'bronze.crm_cust_info',
--         'bronze.leads',
--         'silver.temp_customers'
--     ];
-- BEGIN
--     FOREACH tbl IN ARRAY table_list LOOP
--         PERFORM truncate_if_not_empty(tbl);
--     END LOOP;
-- END $$;