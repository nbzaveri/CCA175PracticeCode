/**
* sbt package
* spark-submit --class "AggregateByTotal" --master local /home/cloudera/scala/target/scala-2.10/aggbytotal-project_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

object AggregateByTotal{
	def main(args: Array[String]){
	
		val conf=new SparkConf().setAppName("AggregateByTotal")
		val sc = new SparkContext(conf)
		
		val orders = sc.textFile("/user/cloudera/tableData/orders")
		orders.count()
		
		val orderItems = sc.textFile("/user/cloudera/tableData/order_items")
		//total Rev
		val totalRev = orderItems.map(rec => (rec.split(",")(4).toDouble)).reduce((acc,value) => acc+value)
		printf("%f", totalRev)
		
		//Total count
		val orderCount = orderItems.map(rec => (rec.split(",")(1).toInt)).distinct().count()
		
		//avg
		totalRev/orderCount
		
		val orderItemsMap = orderItems.map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
		//add rev of same orders
		val revPerOrder = orderItemsMap.reduceByKey((acc, value) => (acc + value))
		//find order having max revenue
		revPerOrder.reduce((acc, value) => (if (acc._2 >= value._2) acc else value))
	}
}