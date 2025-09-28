/*
=============================================================================
DDL Script: Create Gold Views
=============================================================================
This script is for create views for the gold layer using star schema.
*/
-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
if OBJECT_ID('gold.dim_customers','V') is NOT NULL
    drop  VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers as(
    SELECT
        ROW_NUMBER() OVER(order by cst_id) as customer_key,
        ci.cst_id as customer_id,
        ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
        la.CNTRY as country,
        ci.cst_marital_status as maritial_status,
        CASE when ci.cst_gndr != 'N/A' then ci.cst_gndr
            else coalesce(ca.gen,'N/A')
            end as gender,
        ca.bdate as birthdate,
        cst_create_date as create_date  
    FROM silver.crm_cust_info as ci
    LEFT JOIN silver.erp_cust_az12 as ca 
    on ci.cst_key = ca.CID
    LEFT JOIN silver.erp_loc_a101 as la
    ON ci.cst_key = la.CID
);
go
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
if OBJECT_ID('gold.dim_products','V') is NOT NULL
    drop  VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products as(
    select
    ROW_NUMBER() OVER(order by pn.prd_start_dt, pn.prd_key) as product_key,
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pn.cat_id as category_id,
    pc.CAT as category_name,
    pc.SUBCAT as subcategory,
    pc.MAINTENANCE,
    pn.prd_cost as cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
    from silver.crm_prd_info as pn
    LEFT JOIN [silver].[px_cat_g1v2] as pc
    on pn.cat_id = pc.id
    where pn.prd_end_dt is NULL -- current data with no end date
);
go
-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================
if OBJECT_ID('gold.fact_sales','V') is NOT NULL
    drop  VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS (
    SELECT
        sd.sls_ord_num  AS order_number,
        pr.product_key  AS product_key,
        cu.customer_key AS customer_key,
        sd.sls_order_dt AS order_date,
        sd.sls_ship_dt  AS shipping_date,
        sd.sls_due_dt   AS due_date,
        sd.sls_sales    AS sales_amount,
        sd.sls_quantity AS quantity,
        sd.sls_price    AS price
    FROM silver.crm_sales_details sd
    LEFT JOIN gold.dim_products pr
        ON sd.sls_prd_key = pr.product_number
    LEFT JOIN gold.dim_customers cu
        ON sd.sls_cust_id = cu.customer_id
);
GO