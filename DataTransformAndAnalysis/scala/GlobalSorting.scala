* sbt package
* spark-submit --class "GlobalSorting" --master local /home/cloudera/scala/target/scala-2.10/globalsorting_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
object GlobalSorting{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("GlobalSorting")
		val sc = new SparkContext(conf)
		
		val products = sc.textFile("/user/cloudera/tableData/products")
		products.map(rec => (rec.split(",")(4).toFloat, rec)).sortByKey().take(10).foreach(println)
		products.map(rec => (rec.split(",")(4).toFloat, rec)).to(10).foreach(println)
		
		products.takeOrdered(10)(Ordering[Float].reverse.on(rec => rec.split(",")(4).toFloat)).foreach(println)
		products.takeOrdered(10)(Ordering[Float].on(rec => rec.split(",")(4).toFloat)).foreach(println)
		
		//--------------------------------------//
		products.map(rec => (rec.split(",")(1),rec)).groupByKey().take(10).foreach(println)
		
	}
}
