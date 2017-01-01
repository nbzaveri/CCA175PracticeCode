#To submit this program use
#spark-submit --master yarn AggregateByTotal.py

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Aggregate using Total")
sc = SparkContext(conf=conf)
#Count example
ordersRDD = sc.textFile("/user/cloudera/tableData/orders")
print(ordersRDD.count())

#Total example
orderItemsRDD = sc.textFile("/user/cloudera/tableData/order_items")
#take 4th element which is subtotal for that order.
#without float cast, it takes each element as string and + will concanate all strings
orderItemsMap = orderItemsRDD.map(lambda rec: float(rec.split(",")[4]))
#all all subtotal to get total revenue
print(orderItemsMap.reduce(lambda rev1, rev2: rev1+rev2))

#Max-Min example
maxSubTotal = orderItemsRDD.reduce(lambda rec1, rec2: rec1 if (float(rec1.split(",")[4])) >= (float(rec2.split(",")[4])) else rec2)
print(maxSubTotal)

minSubTotal = orderItemsRDD.reduce(lambda rec1, rec2: rec1 if (float(rec1.split(",")[4])) <= (float(rec2.split(",")[4])) else rec2)
print(minSubTotal)

#Avg order revenue calculation
#Take 4th element which is subtoal and add them all
totalRev = orderItemsRDD.map(lambda rec: float(rec.split(",")[4])).reduce(lambda rec1, rec2: rec1+rec2)
#Take 1st element which is order id and get its distinct's count
totalOrders = orderItemsRDD.map(lambda rec: rec.split(",")[1]).distinct().count()

totalRev/totalOrders
