from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from time import sleep
from pyspark.sql.functions import broadcast

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Broadcast Joins") \
    .getOrCreate()


def large_small_join():
    big_df = spark.range(1, 100000000) # single column "id"
    small_df = spark.createDataFrame([Row(1, "first"), Row(2, "second"), Row(3, "third")]).withColumnRenamed("_1", "id")
    # join with a "lookup table"
    super_join = big_df.join(small_df, "id")
    super_join.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, _2#3]
       +- SortMergeJoin [id#0L], [id#6L], Inner
          :- Sort [id#0L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [id=#26]
          :     +- Range (1, 100000000, step=1, splits=12)
          +- Sort [id#6L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#6L, 200), ENSURE_REQUIREMENTS, [id=#25]
                +- Project [_1#2L AS id#6L, _2#3]
                   +- Filter isnotnull(_1#2L)
                      +- Scan ExistingRDD[_1#2L,_2#3]
    """
    super_join.show() # 15-20s


def broadcast_join():
    big_df = spark.range(1, 100000000)
    small_df = spark.createDataFrame([Row(1, "first"), Row(2, "second"), Row(3, "third")]).withColumnRenamed("_1", "id")
    # join with a "lookup table"
    super_join = big_df.join(broadcast(small_df), "id")
    super_join.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L, _2#3]
       +- BroadcastHashJoin [id#0L], [id#6L], Inner, BuildRight, false
          :- Range (1, 100000000, step=1, splits=12)
          +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),false), [id=#24]
             +- Project [_1#2L AS id#6L, _2#3]
                +- Filter isnotnull(_1#2L)
                   +- Scan ExistingRDD[_1#2L,_2#3]
   """
    super_join.show() # 2s


# Spark can detect the size of some data sources, can automatically broadcast
def auto_broadcast():
    big_df = spark.range(1, 100000000)
    small_df = spark.read.option("header", "true").csv("../data/numbers.csv")
    # join with a "lookup table"
    super_join = big_df.join(small_df, "id") # automatically detected to BroadcastHashJoin
    super_join.explain()


if __name__ == '__main__':
    auto_broadcast()
    sleep(1000)