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