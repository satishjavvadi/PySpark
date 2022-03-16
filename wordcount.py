import findspark
findspark.init()

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("wordcount").getOrCreate()
sc = spark.sparkContext

words = sc.textFile("C:\\Users\\user\\Desktop\\test.txt", 4).flatMap(lambda line: line.split(" "))

a = words.map(lambda x: (x, 1))

b = a.reduceByKey(lambda a ,b: a + b)

print("count:", b.collect())
