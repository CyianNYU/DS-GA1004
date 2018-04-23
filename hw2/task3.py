import sys
from csv import reader
from pyspark import SparkContext
def print(res):
	return '{0}\t{1:.2f}, {2:.2f}'.format(res[0],res[1][0],res[1][0]/res[1][1])
sc =SparkContext('local', 'task3')
open = sc.textFile(sys.argv[1], 1)
open = open.mapPartitions(lambda x: reader(x))
open_pair = open.map(lambda x: (x[2], (float(x[12]),1)))
result = open_pair.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
result = result.map(lambda x: print(x))
result.saveAsTextFile('task3.out')
