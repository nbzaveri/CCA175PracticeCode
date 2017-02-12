#Hive Practice Code Snippet

##Basic Hive Commands
```
show databases;
show tables;
select count(1) from order_items;
create database sqoop_import;
describe formatted order_items
```
##Create Databases
```
CREATE DATABASE IF NOT EXISTS retail_ods;
CREATE DATABASE IF NOT EXISTS retail_edw;
```
##Example
```
CREATE DATABASE IF NOT EXISTS cards;
USE cards;
CREATE TABLE deck_of_cards (
COLOR string,
SUIT string.
PIP string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/cloudera/deck_of_cards.txt' INTO TABLE deck_of_cards;

CREATE EXTERNAL TABLE deck_of_cards_ext (
COLOR string,
SUIT string.
PIP string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;
LOCATION '/user/hive/warehouse/cards.db/deck_of_cards'
```
##retail_ods
```
use retail_ods;
CREATE TABLE categories (
category_id int,
category_department_id int,
category_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE customers (
customer_id       int,
customer_fname    string,
customer_lname    string,
customer_email    string,
customer_password string,
customer_street   string,
customer_city     string,
customer_state    string,
customer_zipcode  string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE departments (
department_id int,
department_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE orders (
order_id int,
order_date string,
order_customer_id int,
order_status string
)
PARTITIONED BY (order_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE order_items (
order_item_id int,
order_item_order_id int,
order_item_order_date string,
order_item_product_id int,
order_item_quantity smallint,
order_item_subtotal float,
order_item_product_price float
)
PARTITIONED BY (order_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE orders_bucket (
order_id int,
order_date string,
order_customer_id int,
order_status string
)
CLUSTERED BY (order_id) INTO 16 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE order_items_bucket (
order_item_id int,
order_item_order_id int,
order_item_order_date string,
order_item_product_id int,
order_item_quantity smallint,
order_item_subtotal float,
order_item_product_price float
)
CLUSTERED BY (order_item_order_id) INTO 16 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE products (
product_id int, 
product_category_id int,
product_name string,
product_description string,
product_price float,
product_image string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;
```
##retail_edw
```
-- Create edw tables (following dimension model)
use retail_edw;
CREATE TABLE products_dimension (
product_id int,
product_name string,
product_description string,
product_price float,
product_category_name string,
product_department_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE order_fact (
order_item_order_id int,
order_item_order_date string,
order_item_product_id int,
order_item_quantity smallint,
order_item_subtotal float,
order_item_product_price float
)
PARTITIONED BY (product_category_department string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;
```
##LOAD Example
```
LOAD DATA LOCAL INPATH '/home/cloudera/test.txt' OVERWRITE INTO TABLE test;
```
##To provide write file to retail_dba
```
mysql -u root -p
select user, file_priv from mysql.user;
update mysql.user set file_priv = 'Y' where user = 'retail_dba';
commit;
exit;
sudo service mysqld restart
```
##To load MySQL data into Hive (using hive load from local)
```
MySQL
select * from categories into outfile '/tmp/categories.psv' fields terminated by '|' lines terminated by '\n';
select * from customers into outfile '/tmp/customers.psv' fields terminated by '|' lines terminated by '\n';
select * from departments into outfile '/tmp/departments.psv' fields terminated by '|' lines terminated by '\n';
select * from products into outfile '/tmp/products.psv' fields terminated by '|' lines terminated by '\n';


Hive
load data local inpath '/tmp/categories.psv' overwrite into table categories;
load data local inpath '/tmp/customers.psv' overwrite into table customers;
load data local inpath '/tmp/departments.psv' overwrite into table departments;
load data local inpath '/tmp/products.psv' overwrite into table products;
```
##To load MySQL data into Hive (using hive load from HDFS)
```
hadoop fs -mkdir /user/cloudera/categories
hadoop fs -put /tmp/categories.psv /user/cloudera/categories

Hive
load data inpath '/user/cloudera/categories/*.psv' overwrite into table categories;
```
##Multi step transformation to load data into Hive table
```
create database retail_stage;
use retail_stage;

CREATE TABLE orders_stage (
order_id int,
order_date string,
order_customer_id int,
order_status string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

select * from orders into outfile '/tmp/orders.psv' fields terminated by '|' lines terminated by '\n'; (mysql)

load data local inpath '/tmp/orders.psv' overwrite into table orders_stage;

set hive.exec.dynamic.partition=TRUE;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table retail_ods.orders partition (order_month)
select order_id, order_date, order_customer_id, order_status, substr(order_date, 1, 7) order_month from retail_stage.orders_stage;

use retail_stage;
CREATE TABLE order_items_stage (
order_item_id int,
order_item_order_id int,
order_item_product_id int,
order_item_quantity smallint,
order_item_subtotal float,
order_item_product_price float
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

select * from order_items into outfile '/tmp/order_items.psv' fields terminated by '|' lines terminated by '\n'; (mysql)
load data local inpath '/tmp/order_items.psv' overwrite into table order_items_stage;

insert overwrite table retail_ods.order_items partition (order_month)
select oi.order_item_id, oi.order_item_order_id, o.order_date,
oi.order_item_product_id, oi.order_item_quantity, oi.order_item_subtotal,
oi.order_item_product_price, substr(o.order_date, 1, 7) order_month 
from retail_stage.order_items_stage oi join retail_stage.orders_stage o
on oi.order_item_order_id = o.order_id;
```
