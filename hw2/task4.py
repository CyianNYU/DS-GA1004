import sys
from csv import reader
from pyspark import SparkContext
sc =SparkContext('local', 'task3')
park = sc.textFile(sys.argv[1], 1)
park = park.mapPartitions(lambda x: reader(x))
park_pair = park.map(lambda x: ('NY' if x[16] == 'NY' else 'Other', 1))
result = park_pair.reduceByKey(lambda x,y: x+y)
result = result.map(lambda x: (str(x[0]) + '\t' + (str(x[1]))))
result.saveAsTextFile('task4.out')
