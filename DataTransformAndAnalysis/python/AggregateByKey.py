from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Totak by key")
sc = SparkContext(conf=conf)

ordersRdd = sc.textFile("/user/cloudera/tableData/orders")
#Take order status as key and use anything for value(0 in this case) as we're using countByKey() action to calculate count
ordersMap = ordersRdd.map(lambda rec: (rec.split(",")[3],0))

#countByKey() example
#ordersMap.countByKey() #will return list
for i in ordersMap.countByKey().items():
	print i
	
#groupByKey() example
ordersMap = ordersRdd.map(lambda rec: (rec.split(",")[3],1))
ordersGroup = ordersMap.groupByKey()
ordersGroupCount = ordersGroup.map(lambda rec: (rec[0], sum(rec[1])))
for i in ordersGroupCount.collect():
	print i
	
#reduceByKey() example
orderCount = ordersMap.reduceByKey(lambda x,y: x+y)
for i in orderCount.collect():
	print i