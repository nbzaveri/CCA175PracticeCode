from pyspark import SparkConf,SparkContext

conf=SparkConf().setAppName("Global Ranking & Sorting")
sc = SparkContext(conf=conf)

products = sc.textFile("/user/cloudera/tableData/products")
productMap = products.map(lambda rec: (float(rec.split(",")[4]), rec))
#ascending sort by key
for i in productMap.sortByKey().collect():  print i
#descending sort by key
for i in productMap.sortByKey(False).collect():  print i

#sort by price and then product id
productMap = products.map(lambda rec: ((float(rec.split(",")[4]),int(rec.split(",")[0])), rec))
for i in productMap.sortByKey().collect():  print i

#drop key from results
for i in productMap.sortByKey().map(lambda rec: rec[1]).collect():  print i

#drop API, only descending order
for i in productMap.top(10):	print i

#takeOrdered API, pring 10 results having descending order of price and asceding order for product_id
for i in productMap.takeOrdered(10, lambda rec: (-rec[0][0], rec[0][1])):  print i
