/**
* sbt package
* spark-submit --class "AvgRevPerDay" --master local /home/cloudera/scala/target/scala-2.10/avgrevperday-project_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

object AvgRevPerDay{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("Simple scala program")
		val sc = new SparkContext(conf)
		
		val orders = sc.textFile("/user/cloudera/tableData/orders")
		val orderItems = sc.textFile("/user/cloudera/tableData/order_items")
		
		//Order_id and order_date
		val ordersMap = orders.map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))
		
		//oder_id and order_sub_total
		val orderItemsMap = orderItems.map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
		
		val ordersJoin = orderItemsMap.join(ordersMap)
		
		//order_date & order_id as key
		val dateOrderIdMap = ordersJoin.map(rec => ((rec._2._2, rec._1), rec._2._1))
		val revPerOrderPerDay = dateOrderIdMap.reduceByKey((acc, value) => acc+value)
		
		//drop order_id from key
		val dateAsKeyMap = revPerOrderPerDay.map(rec => (rec._1._1, rec._2))
		
		//define aggregateByKey to get total rev and order count per day
		val revAndOrderCountMap = dateAsKeyMap.aggregateByKey((0.0, 0))((acc, value) => (acc._1+value, acc._2+1), (acc, value) => (acc._1 + value._1, acc._2+value._2))
		
		revAndOrderCountMap.map(rec => (rec._1, round(rec._2._1/rec._2._2)).sortByKey().collect().foreach(println)
	}
}