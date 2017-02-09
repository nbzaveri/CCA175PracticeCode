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
```

