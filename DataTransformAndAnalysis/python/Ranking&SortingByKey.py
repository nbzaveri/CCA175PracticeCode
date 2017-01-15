from pyspark import SparkConf,SparkContext

conf=SparkConf().setAppName("Ranking & Sorting by Key")
sc = SparkContext(conf=conf)

products = sc.textFile("/user/cloudera/tableData/products")
#get product_record_id as key
productMap = products.map(lambda rec: (rec.split(",")[1], rec))

#groupByKey gives values in iterable type, converting it to list so that we can see the values
productGroup = productMap.groupByKey().map(lambda rec: (rec[0], list(rec[1])))