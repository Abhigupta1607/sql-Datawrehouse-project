/*
=====================================================================================
DDL SCRIPT :- creating a BRONZE table 
=====================================================================================
Purpose :-
         This scripts is used for the creating  table in the bronze schemas, and dropping table if
         its already exist
 =======================================================================================   
*/

IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
   DROP TABLE bronze.crm_cust_info;
GO
  CREATE TABLE bronze.crm_cust_info(
	cust_id int,
	cust_key NVARCHAR(50),
	cust_firstname NVARCHAR(50),
	cust_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
   DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id int,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales int,
	sls_quantity int,
	sls_price float
);

IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
   DROP TABLE bronze.crm_prd_info;
GO


CREATE TABLE bronze.crm_prd_info(
  prd_id int,
	prd_key NVARCHAR(100),
	prd_nm NVARCHAR(100),
	prd_cost int,
	prd_line NVARCHAR(100),
	prd_start_date DATETIME,
	prd_end_date DATETIME
	);

IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
   DROP TABLE bronze.erp_loc_a101;
GO
	
	CREATE TABLE bronze.erp_loc_a101(
		 cid NVARCHAR(50),
		 cntry NVARCHAR(50)
	
	);
IF OBJECT_ID('bronze.erp_loc_az12','U') IS NOT NULL
   DROP TABLE bronze.erp_loc_az12;
GO

	CREATE TABLE bronze.erp_loc_az12(
		 cid NVARCHAR(50),
		 bdate DATE,
		 gen NVARCHAR(50)
	
	);
IF OBJECT_ID('bronze.erp_px_cat_glv2','U') IS NOT NULL
   DROP TABLE bronze.erp_px_cat_glv2;
GO

	CREATE TABLE bronze.erp_px_cat_glv2(
		id NVARCHAR(50),
		cat NVARCHAR(50),
		subcat NVARCHAR(50),
		maintenance NVARCHAR(50)
	
	);
