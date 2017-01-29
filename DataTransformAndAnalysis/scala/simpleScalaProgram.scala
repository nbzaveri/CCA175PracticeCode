/**
* sbt package
* spark-submit --class "SimpleApp" --master local /home/cloudera/scala/target/scala-2.10/simple-project_2.10-1.0.jar
**/
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
object SimpleApp{
	def main(args: Array[String]){
		val conf=new SparkConf().setAppName("Simple scala program")
		val sc = new SparkContext(conf)

		val dptsRdd = sc.textFile("/user/cloudera/tableData/departments")
		dpts.count()
		dpts.collect().foreach(println)
		dptsRdd.saveAsTextFile("/user/cloudera/scala")	
	}
}
