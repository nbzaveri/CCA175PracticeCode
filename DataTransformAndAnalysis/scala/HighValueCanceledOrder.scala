* sbt package
* spark-submit --class "HighValueCanceledOrder" --master local /home/cloudera/scala/target/scala-2.10/highvaluecancleorder_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
object HighValueCanceledOrder{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("HighValueCanceledOrder")
		val sc = new SparkContext(conf)
		
		val sqlContext = new HiveContext(sc)
		
		sqlContext.sql("select * from (select o.order_id, sum(oi.order_item_subtotal) as order_item_revenue from orders o join order_items oi on o.order_id = oi.order_item_order_id where o.order_status = 'CANCELED' group by o.order_id) q where order_item_revenue >= 1000").count()
	}
}




