#Avro Practice Code Snippet

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