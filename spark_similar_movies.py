from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
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


movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
                               ])

# Apply ISO-885901 charset
movieNames = spark.read \
      .option("sep", "|") \
      .option("charset", "ISO-8859-1") \
      .schema(movieNamesSchema) \
      .csv("data/u.item")

combined_df = spark.createDataFrame([],schema_2)

# Create DataFrame
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("data/u.data")

moviesDF_columns = moviesDF.select('userID','movieID', 'Rating')

# Create the spark dataframe

movie_groupby_users_1 = moviesDF_columns.groupBy('userID', 'movieID', 'Rating').count().orderBy('userID')\
    .select('userID','movieID', 'Rating').withColumnRenamed("movieID", "movie_1").withColumnRenamed("Rating", "Rating_1")\
        .withColumnRenamed("userID", "userid_1")

movie_groupby_users_2 = moviesDF_columns.groupBy('userID', 'movieID', 'Rating').count().orderBy('userID')\
    .select('userID','movieID', 'Rating').withColumnRenamed("movieID", "movie_2").withColumnRenamed("Rating", "Rating_2")\
        .withColumnRenamed("userID", "userid_2")

def rating_square(rating_1,rating_2):
    
    return lit((rating_1**2)*(rating_2**2))


join_df = movie_groupby_users_1.join(movie_groupby_users_2).filter(movie_groupby_users_1.userid_1 == movie_groupby_users_2.userid_2) \
            .withColumn("Combined Rating", rating_square(movie_groupby_users_1.Rating_1, movie_groupby_users_2.Rating_2))\
            .filter(movie_groupby_users_1.movie_1 != movie_groupby_users_2.movie_2)


# movie choice
movie_id = 127

selection_1 = join_df.select('userid_1', 'movie_1','Rating_1','movie_2','Rating_2','Combined Rating').filter(join_df.movie_1==movie_id)
selection_2 = join_df.filter(join_df.movie_2==movie_id).select('userid_1', col('movie_2').alias('movie_1'), 'Rating_1', col('movie_1')\
                .alias('movie_2'), 'Rating_2', 'Combined Rating')

concat_df = selection_1.union(selection_2).orderBy(desc('Combined Rating')).dropDuplicates(['userid_1', 'movie_1', 'movie_2'])
concat_df.show()

concat_groupby = concat_df.groupBy('movie_1', 'movie_2').agg({'userid_1': 'count', 'Combined Rating': 'avg'}).orderBy(desc('count(userid_1)'))\
        .withColumnRenamed("count(userid_1)", "count")

concat_groupby.show()
occurance = 50

_results = concat_groupby.where(concat_groupby['count'] > occurance)

results = _results.where(concat_groupby['avg(Combined Rating)'] > 200).orderBy(desc(concat_groupby['avg(Combined Rating)']))

results.show()

print(f'My Film: {movieNames.filter(func.col("movieID") == movie_id).select("movieTitle").collect()[0]["movieTitle"]}')

for result in results.collect():
    movie_idx = result['movie_2']
    print(movieNames.filter(func.col("movieID") == movie_idx) \
        .select("movieTitle").collect()[0]['movieTitle'])

spark.stop()
