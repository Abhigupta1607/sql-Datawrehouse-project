/*
=======================================================================================
Stored Procedure :- Load Bronze Layer (Source -> bronze)
=======================================================================================
Script Purpose:-
        This stored procedure load data into the bronze layer schema from external CSV file 
       It perform the following action:-
       -- Truncate the bronze table before loading data 
       -- user the bulk insert command to the load data from the csv file to bronze table 
=========================================================================================
Parameter 
         None
        This stored procedure dose not accept any parameter or return any valuess.

Usage Example:-
  EXEC bronze.load_bronze
*/

exec bronze.load_bronze
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;

    BEGIN TRY

        SET @batch_start_time=GETDATE();

        PRINT '=========================================';
        PRINT '>> Loading Bronze Layer';
        PRINT '=========================================';

        PRINT '>> Inserting bulk data into crm_cust_info';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Datasql\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------';

        PRINT '>> Inserting bulk data into crm_prd_info';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Datasql\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------';

        PRINT '>> Inserting bulk data into crm_sales_details';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Datasql\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------';

        PRINT '>> Inserting bulk data into erp_loc_a101';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Datasql\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------';

        PRINT '>> Inserting bulk data into erp_loc_az12';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_loc_az12;

        BULK INSERT bronze.erp_loc_az12
        FROM 'C:\Datasql\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------';

        PRINT '>> Inserting bulk data into erp_px_cat_glv2';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_px_cat_glv2;

        BULK INSERT bronze.erp_px_cat_glv2
        FROM 'C:\Datasql\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------------------';
       
       SET @batch_end_time=GETDATE();
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------';
    END TRY
    BEGIN CATCH
        PRINT '============================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR STATE  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================';
    END CATCH
END;
