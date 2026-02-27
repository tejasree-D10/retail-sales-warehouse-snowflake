desc table snowflake_sample_data.TPCDS_SF100TCL.CUSTOMER;
desc table snowflake_sample_data.TPCDS_SF100TCL.CUSTOMER_ADDRESS;

use warehouse compute_wh;
use database snowflake_learning_db;
create or replace table silver.customer_dim(
    customer_sk number autoincrement,
    customer_id number,
    state string,
    start_date date,
    end_date date,
    is_current string
);

--initial load
insert into silver.customer_dim
(customer_id,state,start_date,end_date,is_current)
select 
   c.c_customer_sk,
   ca.ca_state,
   current_date,
   null,
   'Y'
   from snowflake_sample_data.tpcds_sf100tcl.customer c
   join snowflake_sample_data.tpcds_sf100tcl.customer_address ca
   on c.c_current_addr_sk=ca.ca_address_sk;

   select * from silver.customer_dim limit 5;

   --create a small stage
   create or replace table silver.customer_stage as
   select customer_id,state
   from silver.customer_dim
   where is_current='Y';


   --manually update one row
   update silver.customer_stage
   set state='TX'
   where customer_id=83369285;

      select * from silver.customer_stage where customer_id=83369285;

--implement stage 2 using merge
merge into silver.customer_dim target
using silver.customer_stage source
on target.customer_id=source.customer_id
and target.is_current='Y'
when matched and target.state  <> source .state then
update set
     target.end_date=current_date,
     target.is_current='N'
when not matched then 
insert(customer_id,state,start_date,end_date,is_current)
values(source.customer_id,source.state,current_date,null,'Y');

select * from silver.customer_dim where customer_id=83369285;

SELECT *
FROM silver.customer_dim
WHERE customer_id = 83369285
ORDER BY start_date;