import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("readParquetFile").getOrCreate()

df = spark.read.parquet("C:\\Users\\user\\Desktop\\userdata1.parquet")
df.printSchema()
df.show(5)