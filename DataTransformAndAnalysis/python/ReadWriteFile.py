#To submit this program use
#spark-submit --master yarn ReadWriteFile.py

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("ReadWriteFile")
sc = SparkContext(conf=conf)
dataRdd = sc.textFile("/user/cloudera/tableData/departments")
for rec in dataRdd.collect():
	print(rec)
#It does create a dir if it's exist.
dataRdd.saveAsTextFile("/user/cloudera/pyspark/departmentsSave") 