--These are queries I had to use to set up the mysql food mart database to work with my data ingestion properly

--create one sales fact table
create table sales_fact_all as select * from sales_fact_1998  union select * from sales_fact_1997;

--add last update timestamp columns to both sales and promotion tables
alter table sales_fact_all add column last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
alter table promotion add column last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

--this allows the following alter to work with 0 value as is not supported for auto increment but we need that first id to stay 0
SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO';

--modify primary key of promotion table to make it so it is always unique when we add test rows later
alter table promotion modify promotion_id INT AUTO_INCREMENT;

--add test rows to sales and promotion tables for incremental loads testing
insert into sales_fact_all (product_id,time_id,customer_id,promotion_id,store_id,store_sales,store_cost,unit_sales) values (1,1,1,1,1,1.0,1.0,1.0);
insert into promotion (promotion_district_id,promotion_name,media_type,cost,start_date,end_date) values (-1,"some_promo","news",100.0,"2018-12-30 00:00:00","2018-12-31 00:00:00");

--selects and deletes we use to reset and check the new data
select * from sales_fact_all where(product_id = 1 and time_id = 1);
delete from sales_fact_all where(product_id = 1 and time_id = 1);
select * from promotion where(promotion_district_id = -1);
delete from promotion where(promotion_district_id = -1);

-- SNOWFLAKE CODE

--QUERY-1
use FOOD_MART_AGG;
select region_id,promotion_id,COST,weekday_sales,weekend_sales from FOOD_MART_AGG.PUBLIC.SALES_AGG;
--QUERY.2
SELECT region_id,promotion_id,cost, max(weekday_sales),max(weekend_sales) from FOOD_MART_AGG.PUBLIC.SALES_AGG GROUP BY region_id,promotion_id,cost;