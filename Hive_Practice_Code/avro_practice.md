#Avro Practice Code Snippet

##Extracting avro schema using avro-tools
```
mkdir avro
cd avro

[Get data in avro format]
sqoop import-all-tables --connect jdbc:mysql://localhost:3306/retail_db --username retail_dba --password cloudera --warehouse-dir  /user/hive/warehouse/retail_stage.db --m 1 --as-avrodatafile

hadoop fs -get /user/hive/warehouse/retail_stage.db/departments .

avro-tools getmeta departments/part-m-00000.avro
avro-tools getschema departments/part-m-00000.avro > departments.avsc
avro-tools tojson departments/part-m-00000.avro > departments.json
avro-tools fromjson departments.json --schema-file departments.avsc
```

##Creating Hive/Impala tables using Avro schema
```
hadoop fs -mkdir /user/cloudera/avsc_files
hadoop fs -put ~/*.avsc /user/cloudera/avsc_files

CREATE EXTERNAL TABLE categories
STORED AS AVRO
LOCATION 'hdfs://quickstart.cloudera/user/hive/warehouse/retail_stage.db/categories'
TBLPROPERTIES ('avro.schema.url'='hdfs://quickstart.cloudera/user/cloudera/avsc_files/categories.avsc');

CREATE EXTERNAL TABLE customers
STORED AS AVRO
LOCATION 'hdfs:///user/hive/warehouse/retail_stage.db/customers'
TBLPROPERTIES ('avro.schema.url'='hdfs://quickstart.cloudera/user/cloudera/avsc_files/customers.avsc');

CREATE EXTERNAL TABLE departments
STORED AS AVRO
LOCATION 'hdfs:///user/hive/warehouse/retail_stage.db/departments'
TBLPROPERTIES ('avro.schema.url'='hdfs://quickstart.cloudera/user/cloudera/avsc_files/departments.avsc');

CREATE EXTERNAL TABLE order_items
STORED AS AVRO
LOCATION 'hdfs:///user/hive/warehouse/retail_stage.db/order_items'
TBLPROPERTIES ('avro.schema.url'='hdfs://quickstart.cloudera/user/cloudera/avsc_files/order_items.avsc');

CREATE EXTERNAL TABLE orders
STORED AS AVRO
LOCATION 'hdfs:///user/hive/warehouse/retail_stage.db/orders'
TBLPROPERTIES ('avro.schema.literal'='{
  "type" : "record",
  "name" : "orders",
  "doc" : "Sqoop import of orders",
  "fields" : [ {
    "name" : "order_id",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_id",
    "sqlType" : "4"
  }, {
    "name" : "order_date",
    "type" : [ "null", "long" ],
    "default" : null,
    "columnName" : "order_date",
    "sqlType" : "93"
  }, {
    "name" : "order_customer_id",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_customer_id",
    "sqlType" : "4"
  }, {
    "name" : "order_status",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "order_status",
    "sqlType" : "12"
  } ],
  "tableName" : "orders"
}
');

CREATE EXTERNAL TABLE products
STORED AS AVRO
LOCATION 'hdfs:///user/hive/warehouse/retail_stage.db/products'
TBLPROPERTIES ('avro.schema.literal'='{
  "type" : "record",
  "name" : "products",
  "doc" : "Sqoop import of products",
  "fields" : [ {
    "name" : "product_id",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "product_id",
    "sqlType" : "4"
  }, {
    "name" : "product_category_id",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "product_category_id",
    "sqlType" : "4"
  }, {
    "name" : "product_name",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "product_name",
    "sqlType" : "12"
  }, {
    "name" : "product_description",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "product_description",
    "sqlType" : "12"
  }, {
    "name" : "product_price",
    "type" : [ "null", "float" ],
    "default" : null,
    "columnName" : "product_price",
    "sqlType" : "7"
  }, {
    "name" : "product_image",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "product_image",
    "sqlType" : "12"
  } ],
  "tableName" : "products"
}
');
```