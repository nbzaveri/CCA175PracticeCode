#To submit this program use
#spark-submit --master yarn WordCount.py

from pyspark import SparkConf, SparkContext

sc = SparkContext(conf=conf)
conf = SparkConf().setAppName("WordCount")

data = sc.textFile("/user/cloudera/pyspark/wordcount.txt")
dataFlatMap = data.flatMap(lambda x: x.split(" "))
dataMap = dataFlatMap.map(lambda x: (x,1))
wordCount = dataMap.reduceByKey(lambda x,y: x+y)
for rec in wordCount.collect():
	print(rec)