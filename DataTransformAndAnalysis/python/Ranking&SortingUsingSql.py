from pyspark.sql import HiveContext

hiveContext = HiveContext(sc)
map1 = hiveContext.sql("select * from products order by product_price desc limit 10")
for i in map1.collect():	print i

for i in map1.collect():	print(i.product_id, i.product_category_id, i.product_price)

#distribute by example, useful for large data set and gives better performance over order by
map2= hiveContext.sql("select * from products distribute by product_category_id sort by product_category_id, product_price desc")
for i in map2.collect():	print(i.product_id, i.product_category_id, i.product_price)