/*
----------------------------------------------------------------
stored procedure: load the data into the bronze layer
----------------------------------------------------------------
script purpose:
    this stored procedure loads data to the bronze layer from the csv file
    it will truncate tables before loading the data using bulk load

parameter:
    none, this file does not take parameter or return any value
*/

USE DataWareHouse;
GO


CREATE or ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    BEGIN TRY
        PRINT'--------------------------------------------';
        PRINT'Loading Bronze Layer';
        PRINT'--------------------------------------------';
        PRINT'Loading crm tables';
        PRINT'--------------------------------------------';


        set @batch_start_time = GETDATE();
        SET @start_time = GETDATE();
        PRINT '>>>  Truncating Table: bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>>>  Inserting data into Table: bronze.crm_cust_info'
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\Desktop\sql\datawarehouse\dwh-me\data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH(
        FIRSTROW = 2,
        fieldterminator = ',',
        TABLOCK
        
        );
        SET @end_time = GETDATE();
        print '>>> load duration: ' + cast (DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT'--------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>>>  Truncating Table: bronze.crm_prd_info'
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>>>  Inserting data into Table: bronze.crm_prd_info'
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\Desktop\sql\datawarehouse\dwh-me\data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH(
        FIRSTROW = 2,
        fieldterminator = ',',
        TABLOCK
        
        );
        set @end_time = GETDATE();
        print '>>> load duration: ' + cast (DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT'--------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>>>  Truncating Table: bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>>>  Inserting data into Table: bronze.crm_sales_details'
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\Desktop\sql\datawarehouse\dwh-me\data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH(
        FIRSTROW = 2,
        fieldterminator = ',',
        TABLOCK
        
        );
        set @end_time = GETDATE();
        print '>>> load duration: ' + cast (DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT'--------------------------------------------';


        PRINT'--------------------------------------------';
        PRINT'Loading erp tables';
        PRINT'--------------------------------------------';


        SET @start_time = GETDATE();
        PRINT '>>>  Truncating Table: bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>>>  Inserting data into Table: bronze.erp_cust_az12'
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\Desktop\sql\datawarehouse\dwh-me\data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH(
        FIRSTROW = 2,
        fieldterminator = ',',
        TABLOCK
        
        );
        set @end_time = GETDATE();
        print '>>> load duration: ' + cast (DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT'--------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>>>  Truncating Table: bronze.px_cat_g1v2'
        TRUNCATE TABLE bronze.px_cat_g1v2;
        PRINT '>>>  Inserting data into Table: bronze.px_cat_g1v2'
        BULK INSERT bronze.px_cat_g1v2
        FROM 'D:\Desktop\sql\datawarehouse\dwh-me\data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH(
        FIRSTROW = 2,
        fieldterminator = ',',
        TABLOCK
        
        );
        set @end_time = GETDATE();
        print '>>> load duration: ' + cast (DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT'--------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>>>  Truncating Table: bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>>>  Inserting data into Table: bronze.erp_loc_a101'
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\Desktop\sql\datawarehouse\dwh-me\data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH(
        FIRSTROW = 2,
        fieldterminator = ',',
        TABLOCK
        
        );
        set @end_time = GETDATE();
        print '>>> load duration: ' + cast (DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT'--------------------------------------------';

        set @batch_end_time = GETDATE();
        PRINT'--------------------------------------------';
        PRINT'loading bronze layer finished with total duration: '+ cast(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds';
        PRINT'--------------------------------------------';
        PRINT'--------------------------------------------';

    END TRY
    BEGIN CATCH
        PRINT'--------------------------------------------';
        PRINT'error occured during loading bronze layer';
        PRINT 'error message: '+ error_message();
        PRINT 'error message' + cast(error_number() as NVARCHAR);
        PRINT 'error message' + cast(error_state() as NVARCHAR);
        PRINT'--------------------------------------------';
    END CATCH
END
GO
EXEC bronze.load_bronze