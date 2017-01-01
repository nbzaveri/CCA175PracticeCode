# Join example in pyspark
# Find out total revenue Per day and Total order per day using HiveContext
#spark-submit --master yarn RevenuePerDayUsingHiveContext.py

from pyspark.sql import HiveContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Join Example")
sc = SparkContext(conf=conf)

sqlContext = HiveContext(sc)
#Bydefault, hiveContext takes 200 tasks. Limiting it to 10
sqlContext.sql("set spark.sql.shuffle.partitions=10");

#round is available only in hivecontext
joinAggData = sqlContext.sql("select o.order_date, round(sum(oi.order_item_subtotal), 2), count(distinct o.order_id) from orders o join order_items oi on o.order_id = oi.order_item_order_id group by o.order_date order by o.order_date")

#joinAggData.saveAsTextFile("/user/cloudera/joinExampleByHiveContext")
for data in joinAggData.collect():
  print(data)