from pyspark import SparkConf,SparkContext

conf=SparkConf().setAppName("Max Rev by Customer per date")
sc = SparkContext(conf=conf)

orders = sc.textFile("/user/cloudera/tableData/orders")
orderItems = sc.textFile("/user/cloudera/tableData/order_items")

#Select order_id and make it key
ordersMap = orders.map(lambda x: (int(x.split(",")[0]), x))
#Select order_item_order_id and make it key so that it can be joined with orders table
orderItemsMap = orderItems.map(lambda x: (int(x.split(",")[1]),x))

orderJoin = orderItemsMap.join(ordersMap)
#Take date & customer id as key and subtotal as value
perDateMaxRevMap = orderJoin.map(lambda rec: ((rec[1][1].split(",")[1],rec[1][1].split(",")[2]),float(rec[1][0].split(",")[4])))
#Add all subtotal for date+cust_id combination
maxRevPerDateMap = perDateMaxRevMap.reduceByKey(lambda x,y: (x+y))

#take date as key and cust_id & total as value
maxRevPerDatePerCust = maxRevPerDateMap.map(lambda rec: (rec[0][0],(rec[0][1],rec[1])))

#find max for given date
maxRevPerDatePerCustFinal = maxRevPerDatePerCust.reduceByKey(lambda x,y: (x if x[1] >= y[1] else y))
for i in maxRevPerDatePerCustFinal.sortByKey().collect():   print i

#2nd way of finding Max using python function
def findMax(x,y):
	if(x[1] >= y[1]):
		return x
	else:
		return y

maxRevPerDatePerCustFinal = maxRevPerDatePerCust.reduceByKey(lambda x,y: findMax(x,y))
for i in maxRevPerDatePerCustFinal.sortByKey().collect():   print i