from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Average By Key")
sc = SparkContext(conf=conf)

orders = sc.textFile("/user/cloudera/tableData/orders")
orderItems = sc.textFile("/user/cloudera/tableData/order_items")

#Select order_id and make it key
ordersMap = orders.map(lambda x: (int(x.split(",")[0]), x))
#Select order_item_order_id and make it key so that it can be joined with orders table
orderItemsMap = orderItems.map(lambda x: (int(x.split(",")[1]),x))

orderJoin = orderItemsMap.join(ordersMap)
#Take orderDate, order_id as key and order_subtotal as value
orderDataMap = orderJoin.map(lambda rec: ((rec[1][1].split(",")[1],rec[0]),float(rec[1][0].split(",")[4])))
orderTotalPerDayPerOrder = orderDataMap.reduceByKey(lambda x,y: x+y)
#Dropping order_id from key
revPerDayPerOrderMap = orderTotalPerDayPerOrder.map(lambda x: (x[0][0], x[1]))

#Adding all subtotal and no of orders per day
revPerDay = revPerDayPerOrderMap.aggregateByKey((0,0), lambda acc, rev: (acc[0]+rev, acc[1]+1), lambda total1, total2: (round(total1[0]+total2[0],2), total1[1], total2[1]))
#calculate avg
avgPerDay = revPerDay.map(lambda rec: (rec[0], rec[1][0]/rec[1][1]))

for i in avgPerDay.sortByKey().collect():
	print i