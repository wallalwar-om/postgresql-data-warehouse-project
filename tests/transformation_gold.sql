-- Here we can see that there are two gender columns
-- So let's do (data integration) with those two
SELECT ci.cst_gndr, ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid;


-- Let's say we were told that crm is master table so changes goes as in crm
-- So here if gen is not in crm then taking it from erp else crm
SELECT DISTINCT ci.cst_gndr, ca.gen,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is Master for gender info
		ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen
	FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
ORDER BY 1, 2;

