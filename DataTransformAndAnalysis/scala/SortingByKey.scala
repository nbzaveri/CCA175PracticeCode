* sbt package
* spark-submit --class "SortingByKey" --master local /home/cloudera/scala/target/scala-2.10/sortingbykey_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
object SortingByKey{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("SortingByKey")
		val sc = new SparkContext(conf)
		
		val products = sc.textFile("/user/cloudera/tableData/products")
		val productGroupBy = products.map(rec => (rec.split(",")(1),rec)).groupByKey()
		productGroupBy.map(rec => (rec._1, rec._2.toList.sortBy(t => t.split(",")(4).toFloat))).take(10).foreach(println)
		
		productGroupBy.flatMap(rec => (rec._2.toList.sortBy(t => t.split(",")(4).toFloat))).take(10).foreach(println)
		//descending order
		productGroupBy.map(rec => (rec._1, rec._2.toList.sortBy(t => -t.split(",")(4).toFloat))).take(10).foreach(println)
		
		def getAll(rec: (String, Iterable[String])) : Iterable[String] = {
			return rec._2
		}
		
		productGroupBy.flatMap(rec => getAll(rec)).take(100).foreach(println)
		productGroupBy.flatMap(getAll(_)).take(100).foreach(println)
		
		def getSorted(rec: (String, Iterable[String])) : Iterable[String] = {
			return rec._2.toList.sortBy(t => t.split(",")(4).toFloat)
		}
		
		productGroupBy.flatMap(rec => getAll(rec)).take(100).foreach(println)
	}
}