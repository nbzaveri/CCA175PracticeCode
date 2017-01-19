from pyspark import SparkConf,SparkContext

conf=SparkConf().setAppName("Ranking & Sorting by Key")
sc = SparkContext(conf=conf)

products = sc.textFile("/user/cloudera/tableData/products")
#get product_record_id as key
productMap = products.map(lambda rec: (rec.split(",")[1], rec))

def getTopDenseN(rec, topN):
	x = [ ]
	topNPrices = [ ]
	prodPrices = [ ]
	prodPricesDesc = [ ]
	#get all prices and get reversed unique prices
	for i in rec[1]:
		prodPrices.append(float(i.split(",")[4]))
	prodPricesDesc = list(sorted(set(prodPrices), reverse=True))
	import itertools
	#get topN price from created list
	topNPrices = list(itertools.islice(prodPricesDesc, 0, topN))
	for j in sorted(rec[1], key=lambda k: float(k.split(",")[4]), reverse=True):
		if(float(j.split(",")[4]) in topNPrices):
			x.append(j)
	return (y for y in x)

for i in productMap.groupByKey().flatMap(lambda x: getTopDenseN(x, 2)).collect(): print(i)