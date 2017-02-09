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

