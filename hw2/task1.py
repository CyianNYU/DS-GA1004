import sys
from csv import reader
from pyspark import SparkContext
sc =SparkContext('local','task1')
park = sc.textFile(sys.argv[1], 1)
park = park.mapPartitions(lambda x: reader(x))
open = sc.textFile(sys.argv[2], 1)
open = open.mapPartitions(lambda x: reader(x))
park_pair = park.map(lambda x: (x[0], (x[14], x[6], x[2], x[1])))
park_pair_0 = park.map(lambda x: (x[0], 1))
open_pair = open.map(lambda x: (x[0], 1))
temp = park_pair.subtractByKey(open_pair)
result = temp.join(park_pair)
#result = result.map(lambda x: (str(x[0])+'\t'+str(x[1])+', '+str(x[2])+', '+str(x[3])+', '+str(x[4])))
def print(x):
	return "{}\t{}, {}, {}, {}".format(x[0],x[1][1][0],x[1][1][1],x[1][1][2],x[1][1][3])
result = result.map(lambda x: print(x))
result.saveAsTextFile('task1.out')
