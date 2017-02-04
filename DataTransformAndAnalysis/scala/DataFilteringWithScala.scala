/**
* sbt package
* spark-submit --class "DataFilteringWithScala" --master local /home/cloudera/scala/target/scala-2.10/datafilteringwithscala_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
object DataFilteringWithScala{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("DataFilteringWithScala")
		val sc = new SparkContext(conf)
		
		val orders = sc.textFile("/user/cloudera/tableData/orders")
		orders.filter(rec => rec.split(",")(3).equals("COMPLETE")).collect().foreach(println)
		orders.filter(_.split(",")(3).contains("PENDING")).collect().foreach(println)
		orders.filter(rec => rec.split(",")(0).toInt > 100).collect().foreach(println)
		orders.filter(_.split(",")(0).toInt == 100).collect().foreach(println)
		orders.filter(rec => rec.split(",")(0).toInt > 10000 || rec.split(",")(3).equals("COMPLETE")).collect().foreach(println)
		orders.filter(rec => rec.split(",")(0).toInt > 10000 && (rec.split(",")(3).equals("CANCELED") || rec.split(",")(3).equals("PENDING"))).collect().foreach(println)
		orders.filter(rec => rec.split(",")(0).toInt > 10000 && !(rec.split(",")(3).equals("COMPLETE"))).collect().foreach(println)
		
		//find all orders which are cancelled and > $1000
		
		//load records, filter canceled records, take order_id as key and entire record as value
		val orders = sc.textFile("/user/cloudera/tableData/orders").filter(rec => rec.split(",")(3).equals("CANCELED")).map(rec => (rec.split(",")(0), rec))
		//load records, take order_id as key and entire record as value
		val orderItems = sc.textFile("/user/cloudera/tableData/order_items").map(rec => (rec.split(",")(1), rec))
		
		//join orders with order_items
		val ordersJoin = orderItems.join(orders)
		
		//keep order_id as key and order_sub_total as value; discard rest
		//add all sub_total for that key
		//filter records having total < 1000
		ordersJoin.map(rec => (rec._1, rec._2._1.split(",")(4).toFloat)).reduceByKey((acc, value) => acc+value).filter(_._2 >= 1000).collect().foreach(println)
	}
}