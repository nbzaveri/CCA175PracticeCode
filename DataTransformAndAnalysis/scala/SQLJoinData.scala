/**
* sbt package
* spark-submit --class "SQLJoinData" --master local /home/cloudera/scala/target/scala-2.10/sqljoindata-project_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext, org.apache.spark.sql.Row

object SQLJoinData{
	def main(args: Array[String]){
		val sqlContext = new SQLContext(sc)

		sqlContext.sql("set spark.sql.shuffle.partitions=10");

		val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
		val ordersMap = ordersRDD.map(o => o.split(","))

		case class Orders(
		  order_id: Int, 
		  order_date: String, 
		  order_customer_id: Int, 
		  order_status: String
		)
		val orders = ordersMap.map(o => Orders(o(0).toInt, o(1), o(2).toInt, o(3)))

		import sqlContext.createSchemaRDD
		orders.registerTempTable("orders")

		case class OrderItems
		  (order_item_id: Int,
		   order_item_order_id: Int,
		   order_item_product_id: Int,
		   order_item_quantity: Int,
		   order_item_subtotal: Float,
		   order_item_product_price: Float
		  )

		val orderItems = sc.textFile("/user/cloudera/sqoop_import/order_items").
		  map(rec => rec.split(",")).
		  map(oi => OrderItems(oi(0).toInt, oi(1).toInt, oi(2).toInt, oi(3).toInt, oi(4).toFloat, oi(5).toFloat))

		orderItems.registerTempTable("order_items")

		val joinAggData = sqlContext.sql("select o.order_date, sum(oi.order_item_subtotal), count(distinct o.order_id) from orders o join order_items oi " +
		  "on o.order_id = oi.order_item_order_id group by o.order_date order by o.order_date")

		joinAggData.collect().foreach(println)
	}
}