/*

===================================================================================================
DLL Scripts: creating a gold view
===================================================================================================
Purpose:
    This scripts creates gold veiw for the gold layer in the dataware-house
    The gold layer represent the final dimesion and fact tables (star schema )

   Each view performs the transformations  and combine the data for silver layer  to produce a clean ,enriched
   and businnes ready data set
======================================================================================================
*/

/*
==============================================================
create view gold.dim_customers
=============================================================
*/

create view gold.dim_customers as
select 
    ROW_NUMBER() over(order by cust_id) as customer_key,
	ci.cust_id as customer_id,
	ci.cust_key as customer_number,
	ci.cust_firstname as firstname ,
	ci.cust_lastname as lastname ,
	la.cntry as country,
	case when ci.cst_gndr!='n/a' then ci.cst_gndr
	     else coalesce(ca.gen,'n/a')
    end as gender,
	ca.bdate as birthday,
	ci.cst_material_status as marital_status,
	ci.cst_create_date
from silver.crm_cust_info ci
LEFT JOIN  silver.erp_loc_az12 ca
on ci.cust_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cust_key=la.cid


/*
==============================================================
create view gold.dim_products
=============================================================
*/

create view gold.dim_products as
select
    row_number() over( order by pn.prd_start_date,pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id ,
	pc.cat as category,
	pc.subcat subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line product_line ,
	pn.prd_start_date as start_date
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_glv2 pc
on pn.cat_id = pc.id
where prd_end_date is null--filter out all historical data

  /*
==============================================================
create view gold.fact_sales
=============================================================
*/

create view gold.fact_sales as
select 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date ,
	sd.sls_ship_dt as shiping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount ,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on  sd.sls_cust_id=cu.customer_id

