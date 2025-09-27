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