from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
spark = SparkSession.builder.appName('PopularSuperHero').getOrCreate()
schema = StructType([StructField('id',IntegerType(),True),
	StructField('name',StringType(), True)])
namesDF = spark.read.option('sep',' ').schema(schema).csv('C:/pyspark_prac/Marvel_Names.txt')
lines = spark.read.text('C:/pyspark_prac/Marvel_Graph.txt')
connectionDF = lines.withColumn('id', functions.split(functions.col('value'), ' ')[0]).withColumn('connection', 
	functions.size(functions.split(functions.col('value'), ' ')) - 1)
connectionDF = connectionDF.groupBy('id').agg(functions.sum('connection').alias('connection'))
mostPopular = connectionDF.sort(functions.col('connection').desc()).first()
mostPopularName = namesDF.filter(namesDF.id == mostPopular[0]).select('name').first()
print(mostPopularName[0]+ " Connection " + str(mostPopular[1]))
spark.stop()