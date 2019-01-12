#!/bin/bash
mysql -u root -proot << EOF
use food_mart;
create table sales_fact_all as select * from sales_fact_1998  union select * from sales_fact_1997;
alter table sales_fact_all add column last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
alter table promotion add column last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO';
alter table promotion modify promotion_id INT AUTO_INCREMENT;
EOF
echo 'food_mart sales and promotions tables updated!'
