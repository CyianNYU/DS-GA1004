import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("task2-sql").config("spark.some.config.option", "some-value").getOrCreate()
park = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
park.createOrReplaceTempView("park")
result = spark.sql("SELECT violation_code, COUNT(summons_number) AS count_summons FROM park \
	GROUP BY violation_code \
	ORDER BY violation_code")
result.select(format_string('%d\t%d',result.violation_code, result.count_summons)).write.save("task2-sql.out",format="text")
