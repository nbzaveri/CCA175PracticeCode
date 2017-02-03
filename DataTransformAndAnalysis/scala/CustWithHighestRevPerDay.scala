/**
* sbt package
* spark-submit --class "CustWithHighestRevPerDay" --master local /home/cloudera/scala/target/scala-2.10/custwithhighrevperday-project_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

object CustWithHighestRevPerDay{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("CustWithHighestRevPerDay")
		val sc = new SparkContext(conf)
		
		val orders = sc.textFile("/user/cloudera/tableData/orders")
		val orderItems = sc.textFile("/user/cloudera/tableData/order_items")
		
		//Order_id as key
		val ordersMap = orders.map(rec => (rec.split(",")(0).toInt, rec))
		
		//oder_id as key
		val orderItemsMap = orderItems.map(rec => (rec.split(",")(1).toInt, rec))
		
		val ordersJoin = orderItemsMap.join(ordersMap)
		//order_date & customer_id as key and order_subtotal as value
		val dateCustIdAsKeyMap = ordersJoin.map(rec => ((rec._2._2.split(",")(1), rec._2._2.split(",")(2).toInt), rec._2._1.split(",")(4).toFloat))
		
		val revPerOrderPerDate = dateCustIdAsKeyMap.reduceByKey((acc, value) => acc+value)
		//move cust_id from key to value
		val dateAsKeyMap = revPerOrderPerDate.map(rec => (rec._1._1, (rec._1._2, rec._2)))
		
		dateAsKeyMap.reduceByKey((acc, value) => if (acc._2 >= value._2) acc else value).sortByKey().collect().foreach(println)
	}
}
