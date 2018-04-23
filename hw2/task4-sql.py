import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("task4-sql").config("spark.some.config.option", "some-value").getOrCreate()
park = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
park.createOrReplaceTempView("park")
result = spark.sql("SELECT CASE WHEN registration_state = 'NY' THEN 'NY' \
	ELSE 'Other' END AS is_ny, COUNT(*) AS num \
	FROM park GROUP BY is_ny ORDER BY is_ny")
result.select(concat(col("is_ny"), lit("\t"), col("num"))).write.save("task4-sql.out",format="text")
