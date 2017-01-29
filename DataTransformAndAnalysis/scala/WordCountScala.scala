/**
* sbt package
* spark-submit --class "WordCountScala" --master local /home/cloudera/scala/target/scala-2.10/wordcount-project_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
object WordCountScala{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("Word Count scala program")
		val sc = new SparkContext(conf)
		
		val data = sc.textFile("/user/cloudera/pyspark/wordcount.txt")
		val dataFlatMap = data.flatMap(rec => rec.split(" "))
		val dataMap = dataFlatMap.map(rec => (rec, 1))
		val reduceByKey = dataMap.reduceByKey((acc, value) => acc+value)
		reduceByKey.collect().foreach(println)
	}
}