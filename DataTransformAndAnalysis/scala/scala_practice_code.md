```
sqlContext.sql("select * from categories").collect().foreach(println)
```
```
sqlContext.sql("select * from categories").count()
```