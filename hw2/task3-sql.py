import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("task3-sql").config("spark.some.config.option", "some-value").getOrCreate()
open_v = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
open_v.createOrReplaceTempView("open_v")
result = spark.sql("SELECT license_type, CAST(SUM(amount_due) AS DECIMAL(16,2)) AS ad, CAST(AVG(amount_due) AS DECIMAL(16,2)) AS ave FROM open_v GROUP BY license_type")
result.select(concat(col("license_type"), lit("\t"), col("ad"), lit(", "), col("ave"))).write.save("task3-sql.out",format="text")
