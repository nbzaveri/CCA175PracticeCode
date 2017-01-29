import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

/*Reading hive data*/
val sqlContext = new HiveContext(sc)
val depts = sqlContext.sql("select * from departments")

depts.collect().foreach(println)


/*Reading JSON data*/
val sqlContext = new SQLContext(sc)
val departmentsJson = sqlContext.jsonFile("/user/cloudera/pyspark/jsonFile.json")

departmentsJson.registerTempTable("departmentsTable")

val departmentsData = sqlContext.sql("select * from departmentsTable")

departmentsData.collect().foreach(println)

//Writing data in json format
departmentsData.toJSON.saveAsTextFile("/user/cloudera/scala/departmentsJson")