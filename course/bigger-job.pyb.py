from pyspark.sql import SparkSession
from time import sleep
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .appName("Bigger Job Practice") \
    .getOrCreate()

sc = spark.sparkContext


"""
    Online store selling used gaming laptops
    laptop: id, make, model, procSpeed (performance score)
    
    Find offers for similar laptops for sale, for every laptop, display the average sale price for all of them.
    Similarity laptop - offer means
        - same make and model
        - processor speed (performance) +/-0.1 points
        
    Example: for laptop(6o0Nfq8,HP,Omen,3.4), I want all offers for HP Omen with performance between 3.3 and 3.5
"""
def laptops_online_store():
    laptops = spark.read.option("header", "true").csv("../data/laptops")
    offers = spark.read.option("header", "true").csv("../data/offers")
    # baseline logic
    joined = laptops.join(offers, ["make", "model"]) \
        .filter(abs(laptops.procSpeed - offers.procSpeed) <= 0.1) \
        .groupBy("registration") \
        .agg(avg("salePrice").alias("averagePrice"))

    joined_2 = laptops.join(offers, (laptops.make == offers.make) & (laptops.model == offers.model) & (abs(laptops.procSpeed - offers.procSpeed) <= 0.1)) \
        .groupBy("registration") \
        .agg(avg("salePrice").alias("averagePrice"))

    # data is skewed! Razer Blade is all over the place.
    # joined.explain()
    # joined.show() # ~15mins

    """
        "salting" - add a new column of random numbers (1 - 100)
        repartition the data into make, model AND salt
    """
    # Bartosz:
    laptops = laptops.withColumn("salt", explode(sequence(lit(1), lit(100))))
    offers = offers.withColumn("salt", round(rand() * 100))

    result_df = (
        laptops
            .join(offers, ["make", "model", "salt"])
            .filter(abs(laptops.procSpeed - offers.procSpeed) <= 0.1)
            .groupBy("registration")
            .agg(avg("salePrice").alias("averagePrice"))
    )

    # result_df.explain()
    # result_df.show() # 2-3 mins

    # Yingying:
    laptops_2 = laptops.withColumn("procSpeed", explode(array(laptops.procSpeed, laptops.procSpeed - 0.1, laptops.procSpeed + 0.1)))
    joined_3 = laptops_2 \
        .join(offers, ["make", "model", "procSpeed"]) \
        .groupBy("registration") \
        .agg(avg("salePrice").alias("averagePrice"))
    joined_3.explain()
    """
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[registration#17], functions=[avg(cast(salePrice#45 as double))])
       +- Exchange hashpartitioning(registration#17, 200), ENSURE_REQUIREMENTS, [id=#102]
          +- HashAggregate(keys=[registration#17], functions=[partial_avg(cast(salePrice#45 as double))])
             +- Project [registration#17, salePrice#45]
                +- SortMergeJoin [make#18, model#19, procSpeed#128], [make#42, model#43, procSpeed#44], Inner
                   :- Sort [make#18 ASC NULLS FIRST, model#19 ASC NULLS FIRST, procSpeed#128 ASC NULLS FIRST], false, 0
                   :  +- Exchange hashpartitioning(make#18, model#19, procSpeed#128, 200), ENSURE_REQUIREMENTS, [id=#94]
                   :     +- Filter isnotnull(procSpeed#128)
                   :        +- Generate explode(array(procSpeed#20, cast((cast(procSpeed#20 as double) - 0.1) as string), cast((cast(procSpeed#20 as double) + 0.1) as string))), [registration#17, make#18, model#19], false, [procSpeed#128]
                   :           +- Project [registration#17, make#18, model#19, procSpeed#20]
                   :              +- Generate explode(org.apache.spark.sql.catalyst.expressions.UnsafeArrayData@8be1a087), [registration#17, make#18, model#19, procSpeed#20], false, [salt#97]
                   :                 +- Filter (isnotnull(make#18) AND isnotnull(model#19))
                   :                    +- FileScan csv [registration#17,make#18,model#19,procSpeed#20] Batched: false, DataFilters: [isnotnull(make#18), isnotnull(model#19)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(make), IsNotNull(model)], ReadSchema: struct<registration:string,make:string,model:string,procSpeed:string>
                   +- Sort [make#42 ASC NULLS FIRST, model#43 ASC NULLS FIRST, procSpeed#44 ASC NULLS FIRST], false, 0
                      +- Exchange hashpartitioning(make#42, model#43, procSpeed#44, 200), ENSURE_REQUIREMENTS, [id=#95]
                         +- Filter ((isnotnull(make#42) AND isnotnull(model#43)) AND isnotnull(procSpeed#44))
                            +- FileScan csv [make#42,model#43,procSpeed#44,salePrice#45] Batched: false, DataFilters: [isnotnull(make#42), isnotnull(model#43), isnotnull(procSpeed#44)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/daniel/dev/rockthejvm/trainings/swissre-spark-optimization..., PartitionFilters: [], PushedFilters: [IsNotNull(make), IsNotNull(model), IsNotNull(procSpeed)], ReadSchema: struct<make:string,model:string,procSpeed:string,salePrice:string>
    """
    joined.show()



if __name__ == '__main__':
    laptops_online_store()
    sleep(3600)