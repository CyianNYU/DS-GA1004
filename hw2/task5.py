import sys
from csv import reader
from pyspark import SparkContext
sc =SparkContext('local', 'task3')
park = sc.textFile(sys.argv[1], 1)
park = park.mapPartitions(lambda x: reader(x))
park_pair = park.map(lambda x: ((x[14], x[16]), 1))
result = park_pair.reduceByKey(lambda x,y: x+y)
result = result.takeOrdered(1, key = lambda x:-x[1])
result = sc.parallelize(result)
result = result.map(lambda x: (str(x[0][0])+", "+str(x[0][1]) + '\t'+str(x[1])))
result.saveAsTextFile('task5.out')
