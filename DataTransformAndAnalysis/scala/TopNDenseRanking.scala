/**
* sbt package
* spark-submit --class "TopNDenseRanking" --master local /home/cloudera/scala/target/scala-2.10/topndenserankig-project_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
object TopNDenseRanking{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("TopNDenseRanking")
		val sc = new SparkContext(conf)
		
		val products = sc.textFile("/user/cloudera/tableData/products")
		val productsGroup = products.map(rec => (rec.split(",")(1), rec)).groupByKey()
		
		def getTopDenseN(rec: (String, Iterable[String]), topN: Int): Iterable[String] = {
		  var prodPrices: List[Float] = List()
		  var topNPrices: List[Float] = List()
		  var sortedRecs: List[String] = List()
		  for(i <- rec._2) {
			prodPrices = prodPrices:+ i.split(",")(4).toFloat
		  }
		  topNPrices = prodPrices.distinct.sortBy(k => -k).take(topN)
		  sortedRecs = rec._2.toList.sortBy(k => -k.split(",")(4).toFloat) 
		  var x: List[String] = List()
		  for(i <- sortedRecs) {
			if(topNPrices.contains(i.split(",")(4).toFloat))
			  x = x:+ i 
		  }
		  return x
		}
		
		productsGroup.flatMap(getTopDenseN(_,2)).foreach(println)
		
		//Spark-Sql
		/*
		* select * from products order by product_category_id, product_price desc
		* select * from products distribute by product_category_id sort by product_category_id, product_price desc //better scalling
		*/
		
		//By key ranking (in Hive we can use windowing/analytic functions)
		/*
		* select * from (select p.*, 
		* dense_rank() over (partition by product_category_id order by product_price desc) dr
		* from products p
		* distribute by product_category_id) q
		* where dr <= 2 order by product_category_id, dr;
		*/
	}
}