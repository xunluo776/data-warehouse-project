/*
----------------------------------------------------------------------
ddl: creates table
----------------------------------------------------------------------
script purpose:
this script creates table on silver schema, it will drop the existing 
tables if they already exist.
----------------------------------------------------------------------
*/
use DataWareHouse;
GO

if OBJECT_ID('silver.crm_cust_info','U') is NOT NULL
DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info
(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO

if OBJECT_ID('silver.crm_prd_info','U') is NOT NULL
DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info
(
    prd_id int,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO

if OBJECT_ID('silver.crm_sales_details','U') is NOT NULL
DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details
(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO

if OBJECT_ID('silver.erp_cust_az12','U') is NOT NULL
DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12
(
    CID NVARCHAR(50),
    BDATE DATE,
    GEN NVARCHAR(50),
    dwh_created_date DATETIME2 DEFAULT GETDATE()
)



if OBJECT_ID('silver.px_cat_g1v2','U') is NOT NULL
DROP TABLE silver.px_cat_g1v2;
CREATE TABLE silver.px_cat_g1v2
(
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50),
    dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO


if OBJECT_ID('silver.erp_loc_a101','U') is NOT NULL
DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101
(
    CID VARCHAR(50),
    CNTRY VARCHAR(50),
    dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO