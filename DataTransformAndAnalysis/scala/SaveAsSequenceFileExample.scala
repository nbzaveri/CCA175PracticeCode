import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output._

val dptsRdd = sc.textFile("/user/cloudera/tableData/departments")
/*writing data as sequence file using Null key*/
dptsRdd.map(x => (NullWritable.get(), x)).saveAsSequenceFile("/user/cloudera/spark/departmentsSeq")

/*writing data as sequece file with key*/
dptsRdd.map(x => (x.split(",")(0), x.split(",")(1))).saveAsSequenceFile("/user/cloudera/spark/departmentsSeq1")

/*writing data as sequence file using new Hadoop API*/
val path="/user/cloudera/spark/departmentsSeq2"
dptsRdd.map(x => (new Text(x.split(",")(0)), new Text(x.split(",")(1)))).saveAsNewAPIHadoopFile(path, classOf[Text], classOf[Text],  classOf[SequenceFileOutputFormat[Text, Text]])

/*Reaching sequence file with key*/
sc.sequenceFile("/user/cloudera/spark/departmentsSeq2",  classOf[IntWritable], classOf[Text]).map(rec => rec.toString()).collect().foreach(println)