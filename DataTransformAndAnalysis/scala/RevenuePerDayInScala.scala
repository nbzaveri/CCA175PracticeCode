/**
* sbt package
* spark-submit --class "RevenuePerDay" --master local /home/cloudera/scala/target/scala-2.10/revenueperday-project_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
object RevenuePerDay{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("Word Count scala program")
		val sc = new SparkContext(conf)
		
		val orderData = sc.textFile("/user/cloudera/tableData/orders")
		val orderItemData = sc.textFile("/user/cloudera/tableData/order_items")
		
		//use order_id as key for both tables so that we can join based on that key
		val orderMap = orderData.map(rec => (rec.split(",")(0).toInt, rec))
		val orderItemMap = orderItemData.map(rec => (rec.split(",")(1).toInt, rec))
		
		//join two tables based on order_id key
		val orderJoinOrderItem = orderItemMap.join(orderMap)
		
		//get date & subtotal as key,value
		val revPerDay = orderJoinOrderItem.map(rec => (rec._2._2.split(",")(1), rec._2._1.split(",")(4).toFloat))
		val revPerDayFinal = revPerDay.reduceByKey((acc, total) => acc+total)
		
		val orderPerDayMap = orderJoinOrderItem.map(rec => (rec._2._2.split(",")(1) + "," + rec._1)).distinct()
		val orderPerDayProcess = orderPerDayMap.map(rec => (rec.split(",")(0), 1))
		val orderPerDay = orderPerDayProcess.reduceByKey((acc, total) => acc+total)
		
		val finalResult = revPerDayFinal.join(orderPerDay)
		finalResult.collect().foreach(println)
		finalResult.count()
	}
}