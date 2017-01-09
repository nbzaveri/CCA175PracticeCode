from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Aggregate by key")
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
#Input & Output type must be same. Combiner & Reducer logic is same
orderCount = ordersMap.reduceByKey(lambda x,y: x+y)
for i in orderCount.collect():
	print i

#combineByKey() example
#Input & Output type is same but combiner & reduce logic can differ
#TODO

	
#aggregateByKey() example
#Input & Output type can be different and combiner & reducer logic can also be different
#TODO