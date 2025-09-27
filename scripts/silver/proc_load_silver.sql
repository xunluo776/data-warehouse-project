/*
==========================================================================
stored procedure: loading silver layer
==========================================================================
this is the ETL process for extracting, cleaning, and loading data from 
bronze layer to silver silver.
the script will truncate and apply the full load method to the the silver table.
==========================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        SET @start_time = GETDATE();
        PRINT '=========================================================================='
        PRINT '-----------------------------------'
        PRINT '>> TRUNCATE TABLE: silver.crm_cust_info'
        PRINT '-----------------------------------'
        TRUNCATE TABLE silver.crm_cust_info;
        -- Loading silver.crm_cust_info
        PRINT '>> INSERT DATA INTO silver.crm_cust_info'
        PRINT '-----------------------------------'
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
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '=========================================================================='
        PRINT '-----------------------------------'
        PRINT '>> TRUNCATE TABLE: silver.crm_prd_info'
        PRINT '-----------------------------------'
        TRUNCATE TABLE silver.crm_prd_info;
        -- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> INSERT DATA INTO silver.crm_prd_info'
        PRINT '-----------------------------------'
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
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '=========================================================================='
        PRINT '-----------------------------------'
        PRINT '>> TRUNCATE TABLE: silver.crm_sales_details'
        PRINT '-----------------------------------'
        TRUNCATE TABLE silver.crm_sales_details;
        SET @start_time = GETDATE();
        PRINT '>> INSERT DATA INTO silver.crm_sales_details'
        PRINT '-----------------------------------'
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

        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '=========================================================================='
        PRINT '-----------------------------------'
        PRINT '>> TRUNCATE TABLE: silver.erp_cust_az12'
        PRINT '-----------------------------------'
        TRUNCATE TABLE silver.erp_cust_az12;
        -- Loading silver.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> INSERT DATA INTO silver.erp_cust_az12'
        PRINT '-----------------------------------'
        INSERT INTO silver.erp_cust_az12 (
            CID,
            BDATE,
            GEN
        )

        select
        case when CID like 'NAS%' then SUBSTRING(CID,4,LEN(cid))
            else CID
        end as CID, 
        CASE when BDATE > GETDATE() then null
            else BDATE
        end as BDATE,
        case when UPPER(trim(gen)) in ('F', 'FEMALE') THEN 'Female'
            when UPPER(trim(gen)) in ('M', 'MALE') THEN 'Male'
            ELSE 'N/A' END AS GEN
        from bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '=========================================================================='
        PRINT '-----------------------------------'
        PRINT '>> TRUNCATE TABLE: silver.erp_loc_a101'
        PRINT '-----------------------------------'
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> INSERT DATA INTO silver.erp_loc_a101'
        PRINT '-----------------------------------'
        -- Loading silver.erp_loc_a101
        SET @start_time = GETDATE();
        INSERT into silver.erp_loc_a101 (
            CID,
            CNTRY
        )
        SELECT
        SUBSTRING(CID,1,2) + SUBSTRING(CID,4,LEN(CID)) as CID,
        CASE WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
            WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
            when TRIM(CNTRY) = 'DE' then 'Germany'
            else trim(CNTRY)
        end as CNTRY
        FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '=========================================================================='
        PRINT '-----------------------------------'
        PRINT '>> TRUNCATE TABLE: silver.px_cat_g1v2'
        PRINT '-----------------------------------'
        TRUNCATE TABLE silver.px_cat_g1v2;
        -- Loading silver.px_cat_g1v2
        PRINT '>> INSERT DATA INTO silver.px_cat_g1v2'
        PRINT '-----------------------------------'
        SET @start_time = GETDATE();

        INSERT INTO silver.px_cat_g1v2 (
        ID,
        CAT,
        SUBCAT,
        MAINTENANCE 
        )

        SELECT
        ID,
        CAT,
        SUBCAT,
        MAINTENANCE
        FROM bronze.px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '=========================================================================='
        set @batch_end_time = GETDATE()
        PRINT 'TOTAL LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@batch_start_time, @batch_end_time) AS NVARCHAR) + ' SECONDS'
        PRINT '=========================================================================='
        PRINT '=========================================================================='
    END TRY
    BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURRED WHEN LOADING SILVER LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END;




