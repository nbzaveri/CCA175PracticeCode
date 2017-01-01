# Join example in pyspark
# Find out total revenue Per day and Total order per day using SQLContext
#spark-submit --master yarn RevenuePerDayUsingSQLContext.py

from pyspark.sql import SQLContext, Row
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Join Example")
sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)
#Bydefault, SQLContext takes 200 tasks. Limiting it to 10
sqlContext.sql("set spark.sql.shuffle.partitions=10");

ordersRDD = sc.textFile("/user/cloudera/tableData/orders")
ordersMap = ordersRDD.map(lambda o: o.split(","))
#Assign each column type
orders = ordersMap.map(lambda o: Row(order_id=int(o[0]), order_date=o[1], order_customer_id=int(o[2]), order_status=o[3]))
ordersSchema = sqlContext.inferSchema(orders)
ordersSchema.registerTempTable("orders")

orderItemsRDD = sc.textFile("/user/cloudera/tableData/order_items")
orderItemsMap = orderItemsRDD.map(lambda oi: oi.split(","))
orderItems = orderItemsMap.map(lambda oi: Row(order_item_id=int(oi[0]), order_item_order_id=int(oi[1]), order_item_product_id=int(oi[2]), order_item_quantity=int(oi[3]), order_item_subtotal=float(oi[4]), order_item_product_price=float(oi[5])))
orderItemsSchema = sqlContext.inferSchema(orderItems)
orderItemsSchema.registerTempTable("order_items")

joinAggData = sqlContext.sql("select o.order_date, sum(oi.order_item_subtotal), count(distinct o.order_id) from orders o join order_items oi on o.order_id = oi.order_item_order_id group by o.order_date order by o.order_date")

#joinAggData.saveAsTextFile("/user/cloudera/joinExampleBySqlContext/")
for data in joinAggData.collect():
  print(data)
