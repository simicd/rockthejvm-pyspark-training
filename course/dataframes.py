from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("DataFrames") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .getOrCreate()

# RDD
# DataFrame (on top of RDD of Rows )
# Row = data structure (tuple) that conforms to a schema (description of fields/columns and types)

# exercise: load the movies DF and select 2 columns of your choice
movies_df = spark.read.json("../data/movies")
# transformations create other DFs
movies_df_2_cols = movies_df.select("Director", "IMDB_Rating") # ANOTHER DF - DFs are immutable
# lazy until we call an ACTION = trigger of the computations of that particular DF (examples: show(), count(), take...)

# expressions
# exercise: read the cars DF and compute (name of the car, weight in kg) - 1kg = 2.2lbs
cars_df = spark.read.json("../data/cars")
cars_kg = cars_df.withColumn("weight_in_kg", cars_df.Weight_in_lbs / 2.2)
#                                            ^^^^^^^^^^^^^^^^^^^^^ column object
cars_kg_2 = cars_df.select("Name", (col("Weight_in_lbs") / 2.2).alias("Weight_in_kg"))
#                                   ^^^^^^^^^^^^^^^^^^^^^^^^^^ column object
cars_kg_3 = cars_df.select(col("Name"), expr("Weight_in_lbs / 2.2")) # expression returning a col object
cars_kg_4 = cars_df.selectExpr("Name", "Weight_in_lbs / 2.2")

# exercise: create a new DF with the total profit of every movie, type COMEDY and with IMDB rating above 6
comedies_profits = movies_df \
    .filter((movies_df.Major_Genre == "Comedy") & (movies_df.IMDB_Rating > 6)) \
    .withColumn("Total_Profits", movies_df.US_Gross + movies_df.Worldwide_Gross + movies_df.US_DVD_Sales) \
    .select("Title", "Total_Profits")

# TODO inspect query plans on this one


# data sources
# exercise: save comedies_profits as 1) TSV file, 2) parquet
def save_formats_exercise():
    comedies_profits.write \
        .option("delimiter", "\t") \
        .option("header", "true") \
        .mode("overwrite") \
        .csv("../data/movies_tsv")

    # parquet default
    comedies_profits.write.save("../data/movies_parquet")

    # dump the data as a Postgres table
    comedies_profits.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public.movies") \
        .save()

# aggregations
"""
    Exercises:
    1. sum up all the profits of all the movies in the DF
    2. count how many distinct directors we have
    3. compute mean/standard deviation of US gross revenue
    4. compute the average IMDB rating AND average US gross revenue PER every director
"""
total_profits_df = movies_df.select((sum("US_Gross") + sum("Worldwide_Gross") + sum("US_DVD_Sales")).alias("Total_MONEY"))
all_aggregations_df = movies_df.groupBy(lit("All movies")).agg(
    sum("Total_Profit"), countDistinct("Director"), mean("US_Gross"), stddev("US_Gross")
)
ratings_per_director = movies_df.groupBy("Director").mean("IMDB_Rating", "US_Gross")

# joins
def transfer_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public." + table_name) \
        .load()

"""
    Exercise: show all employees who were never managers
"""
employees_df = transfer_table("employees")
dept_manager_df = transfer_table("dept_manager")
employees_df.join(dept_manager_df, "emp_no", "left_anti")
# left ANTI join = take everything from the left table which DO NOT have corresponding rows in the right table satisfying the condition
# equivalent: select * from left where NOT EXISTS (select * from right where ...)

# inner, left/right_outer, full_outer, left_anti, left_semi


if __name__ == '__main__':
    save_formats_exercise()