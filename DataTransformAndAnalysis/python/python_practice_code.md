*List table records using HiveContext*
```
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
depts = sqlContext.sql("select * from departments")
for rec in depts.collect():
	print(rec)
```

*How to use JDBC jar & connect to DataSource. List table records using SQLContext*
Search MySQL connector and use it in classpath
```
find / -name "mysql-connector*.jar"
pyspark --driver-class-path /usr/share/java/mysql-connector-java.jar
OR
os.environ['SPARK_CLASSPATH'] = "/usr/share/java/mysql-connector-java.jar"
```
```
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
jdbcUrl = "jdbc:mysql://quickstart.cloudera:3306/retail_db?user=retail_dba&password=cloudera"
df = sqlContext.load(source="jdbc", url=jdbcUrl, dbTable = "departments")

for rec in df.collect():
	print(rec)

df.count()
```

*Read & Write from HDFS*
```
from pyspark import SparkContext
dataRdd = SparkContext.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/tableData/departments")
for i in dataRdd.collect():
	print(i)
```
*Read and write from HDFS as SequnceFileFormat*
```
dataRdd = sc.textFile("/user/cloudera/tableData/departments")
data = dataRdd.map(lambda x : tuple(x.split(",",1)))
for i in data.collect():
	print(i)

for i in dataRdd.map(lambda x : (None, x)).collect():
	print(i)

dataRdd.map(lambda x : tuple(x.split(",",1))).saveAsSequenceFile("/user/cloudera/pyspark/departmentAsSeq")

for rec in sc.sequenceFile("/user/cloudera/pyspark/departmentAsSeq").collect():
	print(rec)

```
*Read & Write from HDFS in HiveContext*
```
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
hiveData = sqlContext.sql("select * from departments").collect()
for i in hiveData:
	print(i)

for i in hiveData:
	print(i.department_id)
```
*Use HiveContext to create table in Hive*
```
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
sqlContext.sql("create table departmentHive as select * from departments")
```
*Read and write JSON file using SQLContext*
```
from pyspark import SQLContext
sqlContext = SQLContext(sc)
departmentsJson = sqlContext.jsonFile("/user/cloudera/pyspark/jsonFile.json")
departmentsJson.registerTempTable("departmentsTable")
departmentsData = sqlContext.sql("select * from departmentsTable")
for rec in departmentsData.collect():
	print(rec)

departmentsData.toJSON().saveAsTextFile("/user/cloudera/pyspark/departmentsJson")
```