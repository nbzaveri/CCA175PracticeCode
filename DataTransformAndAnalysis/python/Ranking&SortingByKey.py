from pyspark import SparkConf,SparkContext

conf=SparkConf().setAppName("Ranking & Sorting by Key")
sc = SparkContext(conf=conf)

products = sc.textFile("/user/cloudera/tableData/products")
#get product_record_id as key
productMap = products.map(lambda rec: (rec.split(",")[1], rec))

#groupByKey gives values in iterable type, converting it to list so that we can see the values
productGroup = productMap.groupByKey().map(lambda rec: (rec[0], list(rec[1])))

#flatmap to get all list value available
#sorted will sort the list
for i in productGroup.flatMap(lambda rec: sorted(rec[1])).collect():	print i

productGroup = productMap.groupByKey()
#sort data based on certain key (here product price is key)
#this provide data sorted by product price per categoty
for i in productGroup.flatMap(lambda rec: sorted(rec[1], key=lambda k: k.split(",")[4])).collect():	print i
#without float converstion it does sorting on string price value
for i in productGroup.flatMap(lambda rec: sorted(rec[1], key=lambda k: float(k.split(",")[4]))).collect():	print i

#descending order
for i in productGroup.flatMap(lambda rec: sorted(rec[1], key=lambda k: float(k.split(",")[4]),reverse=True)).collect():	print i


##Get TopN product by price in each category
def TopNData(rec, topN):
	x = [ ] #crete a blank list
	x = list(sorted(rec[1],key=lambda k: float(k.split(",")[4]),reverse=True)) #apply sorted function
	import itertools
	return (y for y in list(itertools.islice(x,0,topN))) #get topN data
	
for i in productGroup.flatMap(lambda rec: TopNData(rec, 10)).collect():	print i