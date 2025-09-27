
-- Loading silver.crm_cust_info
INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    WHEN  UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    ELSE 'N/A'
END AS cst_marital_status, --NORMALIZE
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN  UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    ELSE 'N/A'
END AS cst_gndr, --NORMALIZE
cst_create_date

FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;


-- Loading silver.crm_prd_info

INSERT INTO silver.crm_prd_info(
    prd_iD,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)

SELECT
prd_id,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost, 0) as prd_cost,
CASE 
	WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	ELSE 'N/A'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
DATEADD(DAY,-1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info;

-- Loading silver.crm_sales_details
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case WHEN sls_order_dt = 0 or LEN(sls_order_dt) !=8 then NULL
    else cast(cast(sls_order_dt as VARCHAR) as date) 
END as sls_order_dt,
case WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) !=8 then NULL
    else cast(cast(sls_ship_dt as VARCHAR) as date) 
END as sls_ship_dt,
case WHEN sls_due_dt = 0 or LEN(sls_due_dt) !=8 then NULL
    else cast(cast(sls_due_dt as VARCHAR) as date) 
END as sls_due_dt,
case when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity*ABS(sls_price)
        then  sls_quantity * abs(sls_price)
    else sls_sales
end as sls_sales_new,
sls_quantity,
case when sls_price is null or sls_price<=0
        then sls_sales/nullif(sls_quantity,0) 
    else sls_price
END as sls_price_new
from bronze.crm_sales_details;



