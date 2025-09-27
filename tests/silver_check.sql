-- ===================================================================
-- Checking 'silver.crm_cust_info'
-- ===================================================================
-- check invalid PK
-- Expectation: No Results
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for extra spaces
-- Expectation: No Results
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Check for marital status
-- Expectation: Single , Married, or N/A
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- ===================================================================
-- Checking 'silver.crm_prd_info'
-- ===================================================================
-- check invalid PK
-- Expectation: No Results
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- check for extra spaces
-- Expectation: No Results
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- check for invalid costs
-- Expectation: No Results
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- check for full name of prd_line
-- Expectation: should have Mountain, Other, Sales, Road, Touring, N/A
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- check if start date is greater than end date
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
-- ===================================================================
-- Checking 'silver.crm_prd_info'
-- ===================================================================

-- check for invalid date
-- Expectation: No Invalid Dates
SELECT
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8;

-- check date where order date > shipping or due dates
-- expectation: No results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- check if the data follow the rule of sales = quantity * price
-- Expectation: No Results
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- CHECK FOR INVALID DATE
-- Expectation: NO OUTPUT
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

-- CHECK THE OUTPUT OF GEN
-- Expectation: Male, Female, N/A
SELECT DISTINCT GEN FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================

-- CHECK THE OUTPUT OF GEN
-- Expectation: FULL COUNTRY NAME
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;