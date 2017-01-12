from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Data Filter examples")
sc = SparkContext(conf=conf)

ordersData = sc.textFile("/user/cloudera/tableData/orders")

#print all complete orders
completeOrder = orderData.filter(lambda line: line.split(",")[3] == "COMPLETE")
for i in completeOrder.collect():	print i

#Print distinct order status
for i in orderData.map(lambda line: line.split(",")[3]).distinct().collect():       print i

#print orders having PENDING as type (includes PENDING and PENDING_PAYMENT)
for i in orderData.filter(lambda line: "PENDING" in line.split(",")[3]).take(100):	print i

#Print orders having order_id > 100
for i in orderData.filter(lambda line: int(line.split(",")[0]) > 100).take(5):   print(i)

#Print orders having order_id > 100 or order_status is any PENDING
for i in orderData.filter(lambda line: int(line.split(",")[0]) > 100 or line.split(",")[3] in "PENDING").take(100):	print i

#Pring orders having order_id > 1000 and (order status either of any PENDING or CANCELED)
for i in orderData.filter(lambda line: int(line.split(",")[0]) > 1000 and ("PENDING" in line.split(",")[3]  or line.split(",")[3] == ("CANCELED"))).take(100):	print i

#Pring orders having order_id > 1000 and order_status is not COMPLETE
for i in orderData.filter(lambda line: int(line.split(",")[0]) > 1000 and line.split(",")[3] != ("COMPLETE")).take(100): print i