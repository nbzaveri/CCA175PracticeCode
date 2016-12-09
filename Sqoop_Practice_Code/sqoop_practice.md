# Sqoop Practice Code Snippet

##Basic Sqoop Commands
```
sqoop list-databases \
--connect jdbc:mysql://quickstart.cloudera:3306 \
--username retail_dba \
--password cloudera
```
```
sqoop list-databases \
--connect jdbc:mysql://quickstart.cloudera:3306 \
--username root \
--password cloudera
```
```
sqoop list-tables \
--connect jdbc:mysql://localhost:3306/retail_db \
--username retail_dba --password cloudera
```
```
sqoop eval \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
--query 'select count(1) from order_items'
```

##Sqoop Import table/all tables commands
**CASE-1 (screenshot-1) (created *.java files in $HOME directory)**
```
sqoop import-all-tables \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 12 \
--warehouse-dir /user/cloudera/sqoop-import
```
**CASE-2 (screenshot-2) (created *.avsc files in $HOME directory)**
```
sqoop import-all-tables \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 4 \
--as-avrodatafile \
--warehouse-dir /user/cloudera/sqoop_import_avro 
```
**CASE-3 (screenshot-3) (created *.java files in $HOME/java_files directory)**
```
sqoop import-all-tables \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 \
--hive-import \
--hive-overwrite \
--create-hive-table \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--outdir java_files
```
**CASE-4 (screenshot-4)**
```
sqoop import-all-tables \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 \
--hive-import \
--hive-overwrite \
--create-hive-table \
--hive-database sqoop_import 
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--target-dir /user/cloudera/import_departments
```
*Sample Data insert for subsequent commands*
```
sqoop eval --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
--query "insert into departments value (8000,'TESTING')"
```
*Data gets skewed in below example as 8000 created using above line is outlier. Example of how MIN/MAX limit is getting used to identify limit*
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--target-dir /user/cloudera/import_departments_1 \
-m 2
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--target-dir /user/cloudera/import_departments_2 \
--boundary-query "select min(department_id), max(department_id) from departments where department_id <> 8000" \
-m 2
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--target-dir /user/cloudera/import_departments_2 \
--boundary-query "select 2,7 from departments" \
-m 2
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--target-dir /user/cloudera/import_departments_2 \
--boundary-query "select 2,7 from departments" \
-m 2 \
--columns department_id,department_name
```
*When there's no PrimaryKey available in table, use --split-by*
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--target-dir /user/cloudera/import_departments_2 \
--boundary-query "select 2,7 from departments_nopk" \
-m 2 \
--split-by department_id 
```
*Provide \$CONDITIONS in your query if you use in sqooop import. This requires to sqoop so that it can add --where clause (see next example*
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
--query "select * from orders join order_items on orders.order_id = order_items.order_item_order_id where \$CONDITIONS" \
--target-dir /user/cloudera/import_order_join \
-m 4 \
--split-by order_id
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
--table departments \
--target-dir /user/cloudera/import_departments_3 \
--where "department_id > 5"
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 \
--table departments \
--hive-import \
--hive-overwrite \
--hive-table depts \
--hive-database sqoop_import
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 \
--table departments \
--hive-import \
--hive-overwrite \
--hive-table sqoop_import.depts
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera -m 1 \
--table departments \
--hive-import \
--hive-overwrite \
--hive-table sqoop_import.depts_test \
--create-hive-table
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 --table departments \
--target-dir /user/hive/warehouse/sqoop_import.db/departments \
--append \
--split-by department_id \
--where "department_id > 7"
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 --table departments \
--target-dir /user/cloudera/sqoop_import/departments \
--append \
--check-column department_id \
--incremental append --last-value 7
```
```
sqoop merge --merge-key department_id \
  --new-data /user/cloudera/sqoop_merge/departments_delta \
  --onto /user/cloudera/sqoop_merge/departments \
  --target-dir /user/cloudera/sqoop_merge/departments_stage \
  --class-name departments  \
  --jar-file \
```

##Sqoop Export Commands
```
sqoop export --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
--table order_items_export \
--export-dir /user/cloudera/sqoop_import/order_items \
--batch --outdir java_files_again
```
```
sqoop export --connect jdbc:mysql://localhost:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 \
--table departments \
--export-dir /user/cloudera/sqoop_import/departments_export/ \
--update-key department_id \
--update-mode allowinsert
```
*By default export uses updateOnly mode and that's why below command will not insert anything, it will just update*
```
sqoop export --connect jdbc:mysql://localhost:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 \
--table departments \
--export-dir /user/cloudera/sqoop_import/departments_export/ \
--update-key department_id
```
*When there's no primary key in table we need to specify --update-key*
```
sqoop export --connect jdbc:mysql://localhost:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 \
--table departments_export \
--export-dir /user/cloudera/sqoop_import/departments_export/ \
--update-key department_id \
--update-mode allowinsert
```

##File delimiter and file format
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 \
--table departments \
--target-dir /user/cloudera/sqoop_import/departments_enclosedBy \
--enclosed-by \"
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 \
--table departments \
--target-dir /user/cloudera/sqoop_import/departments_terminatedBy \
--enclosed-by \" \
--fields-terminated-by \| \
--lines-terminated-by \:
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 1 \
--table departments_test \
--hive-import \
--hive-overwrite -\
-hive-table sqoop_import.departments_test \
--create-hive-table \
--null-string nvl \
--null-non-string -1
```
*Handling delimiter in Export example*
```
sqoop export --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba --password cloudera \
-m 2 \
--table departments_test \
--export-dir /user/hive/warehouse/sqoop_import.db/departments_test  \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' \
--batch \
--outdir java_files \
--input-null-string nvl \
--input-null-non-string -1
```

##Sqoop File Formats
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba \
--password cloudera  \
--table departments	 \
--as-sequencefile \
--target-dir /user/cloudera/departments_sequence
```
```
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username retail_dba \
--password cloudera  \
--table departments	 \
--as-avrodatafile \
--target-dir /user/cloudera/departments_avro
```
*To create Hive table using .avsc format
```
hadoop fs -put departments.avsc /user/cloudera
```
```
CREATE EXTERNAL TABLE departments_avro
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs:///user/cloudera/departments_avro'
TBLPROPERTIES ('avro.schema.url'='hdfs://quickstart.cloudera/user/cloudera/departments.avsc');
```