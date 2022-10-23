from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("DataFrames") \
    .config("spark.jars", "../jars/swissre_spark_udf_example_jar/swissre-spark-udf-example.jar") \
    .getOrCreate()

if __name__ == '__main__':
    df = spark.read.option("header", "true").csv("../data/sample.csv")
    spark.udf.registerJavaFunction("rtjvm_count", "com.rockthejvm.Occurrences", IntegerType())
    df.selectExpr("title", "rtjvm_count(title)").show()