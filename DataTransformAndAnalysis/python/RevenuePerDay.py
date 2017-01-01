# Join example in pyspark
# Find out total revenue Per day and Total order per day
#spark-submit --master yarn RevenuePerDay.py

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Join Example")
sc = SparkContext(conf=conf)

orders = sc.textFile("/user/cloudera/tableData/orders")
orderItems = sc.textFile("/user/cloudera/tableData/order_items")

#Select order_id and make it key
ordersMap = orders.map(lambda x: (int(x.split(",")[0]), x))
#Select order_item_order_id and make it key so that it can be joined with orders table
orderItemsMap = orderItems.map(lambda x: (int(x.split(",")[1]),x))

orderJoin = orderItemsMap.join(ordersMap)

#Take Date and total of that order
revPerDayData = orderJoin.map(lambda x: (x[1][1].split(",")[1], float(x[1][0].split(",")[4])))
revPerDay = revPerDayData.reduceByKey(lambda x,y: x+y)

#Take Date and order_id as key and take their distinct to remove duplicates
ordersPerDay = orderJoin.map(lambda x: (x[1][1].split(",")[1] + "," + str(x[0]))).distinct()
ordersPerDayParsed = ordersPerDay.map(lambda x: (x.split(",")[0],1))
ordersPerDayFinal = ordersPerDayParsed.reduceByKey(lambda x,y: x+y)

#Combining RevPerDay and OrdersPerDay in single RDD
finalCount = ordersPerDayFinal.join(revPerDay)
finalCount.reduceByKey().saveAsTextFile("/user/cloudera/joinExample/")