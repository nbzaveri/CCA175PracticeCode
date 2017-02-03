/**
* sbt package
* spark-submit --class "CustWithHighestRevPerDayHive" --master local /home/cloudera/scala/target/scala-2.10/custwithhighrevperdayhive-project_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

object CustWithHighestRevPerDayHive{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("CustWithHighestRevPerDayHive")
		val sc = new SparkContext(conf)
		val hiveContext = new HiveContext(sc)
		
		//hiveContext.sql("set spark.sql.shuffle.partition=10")
		
		val maxOrderByCustPerDay = hiveContext.sql("select o.order_date, sum(oi.order_item_subtotal)/count(oi.order_item_order_id) from orders o join order_items oi on o.order_id=oi.order_item_order_id group by (o.order_date) order by o.order_date")
		
		maxOrderByCustPerDay.collect().foreach(println)
	}
}