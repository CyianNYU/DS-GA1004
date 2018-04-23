import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("task7-sql").config("spark.some.config.option", "some-value").getOrCreate()
park = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
park.createOrReplaceTempView("park")
park = spark.sql("SELECT violation_code, issue_date FROM park")
park.createOrReplaceTempView("park")
violation_all = spark.sql("SELECT violation_code FROM park GROUP BY violation_code")
violation_all.createOrReplaceTempView("violation_all")
park = spark.sql("SELECT violation_code, CAST(issue_date AS DATE) AS issue_date FROM park")
park.createOrReplaceTempView("park")
park = spark.sql("SELECT violation_code, CAST(issue_date AS STRING) AS issue_date FROM park")
park.createOrReplaceTempView("park")
end = spark.sql("SELECT v.violation_code AS end_code, CASE WHEN COUNT(p.violation_code) IS NULL THEN 0.00 ELSE COUNT(p.violation_code) END AS end_count FROM violation_all v LEFT JOIN park p ON v.violation_code = p.violation_code AND p.issue_date IN ('2016-03-05','2016-03-06','2016-03-12','2016-03-13','2016-03-19','2016-03-20','2016-03-26','2016-03-27') GROUP BY v.violation_code")
end.createOrReplaceTempView("end")
day = spark.sql("SELECT v.violation_code AS day_code, CASE WHEN COUNT(p.violation_code) IS NULL THEN 0.00 ELSE COUNT(p.violation_code) END AS day_count FROM violation_all v LEFT JOIN park p ON v.violation_code = p.violation_code AND p.issue_date NOT IN ('2016-03-05','2016-03-06','2016-03-12','2016-03-13','2016-03-19','2016-03-20','2016-03-26','2016-03-27') GROUP BY v.violation_code")
day.createOrReplaceTempView("day")
result = spark.sql("SELECT end_code AS vc, CAST(end_count/8.0 AS DECIMAL(10,2)) AS end_count, CAST(day_count/23.0 AS DECIMAL(10,2)) AS day_count FROM end LEFT JOIN day ON end.end_code= day.day_code ORDER BY end_code")
result.select(concat(col("vc"), lit("\t"), col("end_count"), lit(", "), col("day_count"))).write.save("task7-sql.out", format = "text")