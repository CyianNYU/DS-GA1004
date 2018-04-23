import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("task6-sql").config("spark.some.config.option", "some-value").getOrCreate()
park = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
park.createOrReplaceTempView("park")
result = spark.sql("SELECT plate_id, registration_state, COUNT(*) AS max_count FROM park GROUP BY plate_id, registration_state ORDER BY COUNT(*) DESC, plate_id LIMIT 20")
result.select(concat(col("plate_id"),lit(", "),col("registration_state"),lit("\t"), col("max_count"))).write.save("task6-sql.out", format = "text")

