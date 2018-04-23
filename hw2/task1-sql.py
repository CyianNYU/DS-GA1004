import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("task1-sql").config("spark.some.config.option", "some-value").getOrCreate()
park=spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
open_v = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
park.createOrReplaceTempView("park")
open_v.createOrReplaceTempView("open_v")
result = spark.sql("SELECT p.summons_number, p.plate_id, p.violation_precinct, p.violation_code, p.issue_date FROM park p \
	LEFT JOIN open_v o ON p.summons_number = o.summons_number WHERE o.summons_number IS NULL")
result.select(format_string('%d\t%s, %d, %d,%s',result.summons_number,result.plate_id,result.violation_precinct,result.violation_code,date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")
