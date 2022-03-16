import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("readCsvFile").getOrCreate()
df = spark.read.options(header='True').csv("C:\\Users\\user\\Desktop\\demo_emp.csv")
df.printSchema()
df.show(5)