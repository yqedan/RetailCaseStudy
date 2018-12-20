create table sales_fact_all as select * from sales_fact_1998  union select * from sales_fact_1997;

alter table sales_fact_all add column last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

insert into sales_fact_all (product_id,time_id,customer_id,promotion_id,store_id,store_sales,store_cost,unit_sales) values (1,1,1,1,1,1.0,1.0,1.0);

