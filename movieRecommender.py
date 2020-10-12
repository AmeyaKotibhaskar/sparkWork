from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def calculateSimilarity(spark,moviePairs):
	pairScores = moviePairs.withColumn('xx', func.col('rating1') * func.col('rating1')).withColumn('yy', func.col('rating2') * func.col('rating2')).withColumn('xy', func.col('rating1') * func.col('rating2'))
	# Compute numerator, denominator and numPairs columns
	calculateSimilarity = pairScores.groupBy('movie1', 'movie2').agg(func.sum(func.col('xy')).alias('numerator'),(func.sqrt(func.sum(func.col('xx'))) * func.sqrt(func.sum(func.col('yy')))).alias('denominator'),func.count(func.col('xy')).alias('numPairs'))
	# Calculate score and select only needed columns (movie1, movie2, score, numPairs)
	result = calculateSimilarity.withColumn('score',func.when(func.col('denominator') != 0, func.col('numerator') / func.col('denominator')).otherwise(0)).select('movie1', 'movie2', 'score', 'numPairs')
	return result
	

def getMovieName(movieName, movieId):
	result = movieName.filter(func.col("movie_id") == movieId).select("movie_title").collect()[0]
	return result[0]

#Start of the program
spark = SparkSession.builder.master('local[*]').appName('MovieRecommenderSystem').getOrCreate()

#schema for u.item file
movieNameSchema = StructType([StructField('movie_id', IntegerType(),True),
	StructField('movie_title', StringType(), True)])

#Schema for u.data file
ratingSchema = StructType([StructField('user_id', IntegerType(), True),
	StructField('movie_id', StringType(), True),
	StructField('rating', IntegerType(), True),
	StructField('timestamp', LongType(), True)])

movieName = spark.read.option('sep','|').option('charset','ISO-8859-1').schema(movieNameSchema).csv('C:/pyspark_prac/ml-100k/u.item')

ratings = spark.read.option('sep', '\t').schema(ratingSchema).csv('C:/pyspark_prac/ml-100k/u.data')

ratings = ratings.select('user_id', 'movie_id', 'rating')

moviePairs = ratings.alias('rating1').join(ratings.alias('rating2'),
	(func.col('rating1.user_id') == func.col('rating2.user_id')) 
	& (func.col('rating1.movie_id') < func.col('rating2.movie_id'))).select(func.col('rating1.movie_id').alias('movie1'),func.col('rating2.movie_id').alias('movie2'),
	func.col('rating1.rating').alias('rating1'), 
	func.col('rating2.rating').alias('rating2'))

moviePairSimilarity = calculateSimilarity(spark,moviePairs)
if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarity.filter(((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &(func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)
    
    print ("Top 10 similar movies for " + getMovieName(movieName, movieID))
    
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(getMovieName(movieName, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numPairs))

spark.stop()