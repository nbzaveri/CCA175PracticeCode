from pyspark import SparkConf,SparkContext

conf=SparkConf().setAppName("Cancelled Orders with high rev")
sc = SparkContext(conf=conf)

orders = sc.textFile("/user/cloudera/tableData/orders")
orderItems = sc.textFile("/user/cloudera/tableData/order_items")

#filter CANCELED records from orders
ordersData = orders.filter(lambda rec: rec.split(",")[3] in "CANCELED").map(lambda rec: (int(rec.split(",")[0]), rec))
#get order_id as key and sub_total as value
orderItemsData=orderItems.map(lambda rec: (int(rec.split(",")[1]), float(rec.split(",")[4])))
#aggregate all subtotals and filter records with >1000 value
orderItemsAgg = orderItemsData.reduceByKey(lambda x,y: (x+y)).filter(lambda rec: rec[1] >= 1000)

ordersJoinOrderItems = orderItemsAgg.join(ordersData)
ordersJoinOrderItems.count() #139 records
for i in ordersJoinOrderItems.collect():	print i

#--------------Different way of doing it-------------
#get order_id as key and sub_total as value
orderItemsData=orderItems.map(lambda rec: (int(rec.split(",")[1]), float(rec.split(",")[4])))
#aggregate all subtotals 
orderItemsAgg = orderItemsData.reduceByKey(lambda x,y: (x+y))

ordersJoinOrderItems = orderItemsAgg.join(ordersData)
findRecords = ordersJoinOrderItems.filter(lambda rec: rec[1][0]] >= 1000)
findRecords.count() #139 records

for i in findRecords.collect():	print i
