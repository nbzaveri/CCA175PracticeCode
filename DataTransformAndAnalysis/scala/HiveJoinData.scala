/**
* sbt package
* spark-submit --class "HiveJoinData" --master local /home/cloudera/scala/target/scala-2.10/hivejoindata-project_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

object HiveJoinData{
	def main(args: Array[String]){
		val sqlContext = new HiveContext(sc)
		sqlContext.sql("set spark.sql.shuffle.partitions=10"); 

		val joinAggData = sqlContext.sql("select o.order_date, round(sum(oi.order_item_subtotal), 2), count(distinct o.order_id) " +
						  "from orders o join order_items oi on o.order_id = oi.order_item_order_id " +
						  "group by o.order_date order by o.order_date")
		joinAggData.collect().foreach(println)
	}
}