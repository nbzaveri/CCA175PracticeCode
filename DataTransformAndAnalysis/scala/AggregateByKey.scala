/**
* sbt package
* spark-submit --class "AggregateByKey" --master local /home/cloudera/scala/target/scala-2.10/aggregatebykey-project_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

object AggregateByKey{
	def main(args: Array[String]){
	
		val conf=new SparkConf().setAppName("AggregateByKey")
		val sc = new SparkContext(conf)
		
		val orders = sc.textFile("/user/cloudera/tableData/orders")
		//countByKey()
		orders.map(rec => (rec.split(",")(3), rec)).countByKey().foreach(println)
		
		val ordersMap = orders.map(rec => (rec.split(",")(3), 1))
		//groupByKey()
		ordersMap.groupByKey().map(rec => (rec._1, rec._2.sum)).collect().foreach(println)
		
		//reduceByKey() - need to have same combine & reduce logic as well input/output datatype should be same
		ordersMap.reduceByKey((acc, value) => acc + value).collect().foreach(println)\
		
		//combineByKey() when tuple is 1 based. Can have different combine & reduce logic but input/output data type should be same
		val ordersMap = orders.map(rec => (rec.split(",")(3), 1))
		val ordersResult = ordersMap.combineByKey(value=>1, (acc : Int, value : Int)=> acc+value, (acc: Int, value: Int) => acc+value)
		ordersResult.collect().foreach(println)
		
		//combineByKey() when tuple is 0 based
		val ordersMap = orders.map(rec => (rec.split(",")(3), 0))
		val ordersResult = ordersMap.combineByKey(value=>1, (acc : Int, value : Int)=> acc+1, (acc: Int, value: Int) => acc+value)
		ordersResult.collect().foreach(println)
		
		//aggregateByKey() when tuple is 1 based - can have different combine & reduce logic. Can also have different input/output datatype
		val ordersMap = orders.map(rec => (rec.split(",")(3), 1))
		val ordersResult = ordersMap.aggregateByKey(0)((acc, value)=> acc+1, (acc, value) => acc+value)
		ordersResult.collect().foreach(println)
		
		//aggregateByKey() 
		val ordersMap = orders.map(rec => (rec.split(",")(3), rec))
		val ordersResult = ordersMap.aggregateByKey(0)((acc, value)=> acc+1, (acc, value) => acc+value)
		ordersResult.collect().foreach(println)
	}
}
