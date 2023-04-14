from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

schema_2 = StructType([ \
                     StructField("movie_1", IntegerType(), True), \
                     StructField("Rating_1", IntegerType(), True), \
                     StructField("movie_2", IntegerType(), True), \
                     StructField("Rating_2", IntegerType(), True),\
                     StructField("Combined Rating", IntegerType(), True)
                     ])

combined_df = spark.createDataFrame([],schema_2)

# Create DataFrame
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("data/u.data")

moviesDF_columns = moviesDF.select('userID','movieID', 'Rating')

# print(moviesDF.orderBy(moviesDF.userID.desc()).show())

  
# Create the spark dataframe

movie_groupby_users_1 = moviesDF_columns.groupBy('userID', 'movieID', 'Rating').count().orderBy('userID')\
    .select('movieID', 'Rating').withColumnRenamed("movieID", "movie_1").withColumnRenamed("Rating", "Rating_1")

movie_groupby_users_2 = moviesDF_columns.groupBy('userID', 'movieID', 'Rating').count().orderBy('userID')\
    .select('movieID', 'Rating').withColumnRenamed("movieID", "movie_2").withColumnRenamed("Rating", "Rating_2")

def rating_square(rating_1,rating_2):
    
    return lit((rating_1**2)*(rating_2**2))


join_df = movie_groupby_users_1.join(movie_groupby_users_2).filter(movie_groupby_users_1.movie_1 != movie_groupby_users_2.movie_2)\
            .withColumn("Combined Rating", rating_square(movie_groupby_users_1.Rating_1, movie_groupby_users_2.Rating_2))

# join_df.show()
selection_1 = join_df.filter(join_df.movie_1==50)
selection_2 = join_df.filter(join_df.movie_2==50).select(col('movie_2').alias('movie_1'), 'Rating_1', col('movie_1').alias('movie_2'), 'Rating_2', 'Combined Rating')


concat_df = selection_1.union(selection_2).orderBy(desc('Combined Rating')).groupBy('movie_1', 'movie_2').count()
concat_df.show()
spark.stop()
