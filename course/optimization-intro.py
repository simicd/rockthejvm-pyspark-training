from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Optimization") \
    .config("spark.jars", "../jars/postgresql-42.2.19.jar") \
    .getOrCreate()

sc = spark.sparkContext


def compare_spark_apis():
    df = spark.range(1, 10000000)
    rdd = sc.parallelize(range(1, 10000000))
    print(df.count()) # forcing the evaluation of the DF - 1.2s
    print(rdd.count()) # ... RDD - 2s
    print(df.rdd.count()) # DF -> RDD transformation - 18s
    print(spark.createDataFrame(rdd, IntegerType()).count()) # RDD -> DF transformation ~17s

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


def demo_query_plans():
    simple_numbers = spark.range(1, 1000000) # DF with a column "id"
    simple_numbers_x5 = simple_numbers.selectExpr("id * 5 as newvalue")
    simple_numbers_x5.explain()
    """
        == Physical Plan ==
        *(1) Project [(id#0L * 5) AS newvalue#2L]
        +- *(1) Range (1, 1000000, step=1, splits=12)
    """

    more_numbers = spark.range(1, 1000000, 2)
    more_numbers_split7 = more_numbers.repartition(7)
    more_numbers_x5 = more_numbers_split7.selectExpr("id * 5 as newvalue")
    more_numbers_x5.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [(id#4L * 5) AS newvalue#8L]
       +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [id=#17]
          +- Range (1, 1000000, step=2, splits=12)
    """

    # creating a DF out of an RDD forgets its lineage
    # spark.createDataFrame(more_numbers_x5.rdd, more_numbers_x5.schema).explain()

    df1 = spark.range(1, 1000000)
    df2 = spark.range(1, 100000, 2)
    df3 = df1.repartition(7)
    df4 = df2.repartition(13)
    df5 = df3.selectExpr("id * 3 as id")
    joined = cutLineage(df4.join(df5, "id").union(df2))
    sum_df = joined.selectExpr("sum(id)")
    sum_df.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[], functions=[sum(id#12L)])
       +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#73]
          +- HashAggregate(keys=[], functions=[partial_sum(id#12L)])
             +- Union
                :- Project [id#12L]
                :  +- SortMergeJoin [id#12L], [id#18L], Inner
                :     :- Sort [id#12L ASC NULLS FIRST], false, 0
                :     :  +- Exchange hashpartitioning(id#12L, 200), ENSURE_REQUIREMENTS, [id=#64]
                :     :     +- Exchange RoundRobinPartitioning(13), REPARTITION_BY_NUM, [id=#52]
                :     :        +- Range (1, 10000000, step=2, splits=12)
                :     +- Sort [id#18L ASC NULLS FIRST], false, 0
                :        +- Exchange hashpartitioning(id#18L, 200), ENSURE_REQUIREMENTS, [id=#65]
                :           +- Project [(id#10L * 3) AS id#18L]
                :              +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [id=#54]
                :                 +- Range (1, 100000000, step=1, splits=12)
                +- Range (1, 10000000, step=2, splits=12)
    """

def demo_first_pushdown():
    numbers = spark.range(1, 10000)
    evens = numbers.selectExpr("id as NUMBER").filter(col("NUMBER") % 2 == 0)
    evens.explain(True)

# pushdown can cross stages
def demo_pushdown_on_group():
    numbers = spark.range(1, 10000).withColumnRenamed("id", "NUMBER").withColumn("modulus", expr("NUMBER % 5"))
    grouping = Window.partitionBy("modulus").orderBy("NUMBER")
    ranked = numbers.withColumn("rank", rank().over(grouping)).filter(expr("modulus > 2"))
    ranked.explain(True)


def transfer_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/rtjvm") \
        .option("user", "docker") \
        .option("password", "docker") \
        .option("dbtable", "public." + table_name) \
        .load()


# predicate pushdown all the way to the data source
def demo_pushdown_jdbc():
    employees = transfer_table("employees")
    salaries = transfer_table("salaries")

    max_salary_per_employee = salaries.groupBy("emp_no").agg(max("salary").alias("max_salary"))
    employees_salaries = employees.join(max_salary_per_employee, "emp_no")
    new_employees = employees_salaries.filter("hire_date > '1999-01-01'")
    new_employees.explain(True)


if __name__ == '__main__':
    demo_query_plans()
