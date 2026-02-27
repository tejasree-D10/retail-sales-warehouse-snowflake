
use DATABASE SNOWFLAKE_SAMPLE_DATA;
select count(*)
from tpcds_sf100tcl.store_sales;
use warehouse COMPUTE_WH;
use database SNOWFLAKE_LEARNING_DB;

create schema if not exists BRONZE;
create or replace table BRONZE.STORE_SALES_RAW as
select *
from snowflake_sample_data.tpcds_sf100tcl.store_sales
limit 50000000;
--phase3
select count(*) from bronze.store_sales_raw;
select count(*)
from snowflake_sample_data.tpcds_sf100tcl.store_sales;

select 
count(*) as total_rows,
count(ss_sales_price) as non_null_sales_price
from bronze.store_sales_raw;