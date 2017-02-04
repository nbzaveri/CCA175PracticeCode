/**
* sbt package
* spark-submit --class "DataFilteringWithScala" --master local /home/cloudera/scala/target/scala-2.10/datafilteringwithscala_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
object DataFilteringWithScala{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("DataFilteringWithScala")
		val sc = new SparkContext(conf)
	}
}