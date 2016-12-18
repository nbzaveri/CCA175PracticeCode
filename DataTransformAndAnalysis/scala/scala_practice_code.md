```
sqlContext.sql("select * from categories").collect().foreach(println)
```
sqlContext.sql("select * from categories").count()

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
depts = sqlContext.sql("select * from departments")
for rec in depts.collect():
	print(rec)