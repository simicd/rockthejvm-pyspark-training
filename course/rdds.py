from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("RDD Practice") \
    .getOrCreate()

# spark context = entry point for the RDD (low level) API
sc = spark.sparkContext

# read movies DF as an RDD
# 1 - load a DF and then expose the underlying RDD
movies_df = spark.read.json("../data/movies")
movies_rdd = movies_df.rdd # RDD of Rows
# 2 - load the files directly
movies_rdd_2 = sc.textFile("../data/movies/movies.json") # RDD of strings

# bad of RDD: you can't use the "sql"-like/DataFrame API
# good of RDD: you have higher control over the data, can express computations that are impossible in the DF API

"""
    Transformations:
    - map
    - filter
    - min, max (for RDDs of values which are comparable)
    - reduce
    - groupBy
    - join (for key-value/tuple RDDs)
"""
long_strings_rdd = movies_rdd_2.filter(lambda string: len(string) > 10)
string_lengths_rdd = movies_rdd_2.map(lambda string: len(string)) # obtain an RDD of a different type

"""
    Exercise: compute average IMDB rating by genre on the movies RDD
"""

avg_by_genre_df = movies_df.groupBy("Major_Genre").avg("IMDB_Rating")

avg_ratings_rdd = movies_rdd \
    .filter(lambda row: row.IMDB_Rating is not None and row.Major_Genre is not None) \
    .map(lambda row: (row.Major_Genre, row.IMDB_Rating)) \
    .groupByKey() \
    .mapValues(lambda values: sum(values) / len(values))

"""
Steps:
    - clean up the data with .filter => RDD[Row]
    - for every row, keep the genre and the rating => RDD[(String, Double)]
    - group by key => RDD[(String, List[Double])]
    - map values to compute the average rating per every key => RDD[(String, Double)]
"""

# laziness, immutability
# narrow + wide transformations
# also apply to RDDs as well as DFs

if __name__ == '__main__':
    print(avg_ratings_rdd.collect())
    avg_by_genre_df.show()

