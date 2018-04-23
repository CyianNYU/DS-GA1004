import sys
from csv import reader
from pyspark import SparkContext
sc =SparkContext('local', 'task7')
park = sc.textFile(sys.argv[1], 1)
park = park.mapPartitions(lambda x: reader(x))
def is_weekend(x): 
	weekend = ['05', '06', '12', '13', '19', '20', '26', '27'] 
	key = str(x[1].split('-')[2]) 
	if key in weekend: 
		return (x[2],(1,0))
	else: 
		return (x[2],(0,1))
park_pair = park.map(lambda x: is_weekend(x)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
result = park_pair.map(lambda x: (str(x[0])+"\t"+'{0:.2f}'.format(x[1][0]/8) + ', '+'{0:.2f}'.format(x[1][1]/23)))
result.saveAsTextFile('task7.out')
