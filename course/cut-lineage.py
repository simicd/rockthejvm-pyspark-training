from pyspark.sql import DataFrame, SparkSession

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Cut Lineage") \
    .getOrCreate()


def cutLineage(df):
    # use the underlying Java RDD without round trip to Python
    java_rdd = df._jdf.toJavaRDD()
    # keep schema (JVM version)
    java_schema = df._jdf.schema()
    # cache it, otherwise it gets reused
    java_rdd.cache()
    # access the API entry point for the `createDataFrame`
    sql_context = df.sql_ctx
    try:
        java_sql_context = sql_context._jsqlContext
    except:
        java_sql_context = sql_context._ssql_ctx
    # create new Java DataFrame out of the Java RDD
    new_java_df = java_sql_context.createDataFrame(java_rdd, java_schema)
    # create a PySpark DataFrame based on the existing (new) Java DF
    new_df = DataFrame(new_java_df, sql_context)
    return new_df


if __name__ == '__main__':
    df = spark.range(1000)
    df.explain()
    cutLineage(df).explain()