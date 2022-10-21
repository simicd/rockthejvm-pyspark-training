from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from time import sleep
from pyspark.sql.functions import *
from pyspark.storagelevel import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Caching and Checkpointing") \
    .getOrCreate()

sc = spark.sparkContext

def demo_caching():
    people_df = spark.read.option("sep", ":").csv("../data/people-1m").withColumnRenamed("_c6", "salary")

    # simulate some expensive transformation
    highest_paid_df = people_df.orderBy(desc("salary"))

    # cache the DF so that only the first reference is expensive, subsequent ones are cheap (fetch from memory/storage)
    highest_paid_df.cache() # default mechanism: store in memory (on the JVM)

    # to select the storage option, use persist(...)
    highest_paid_df.persist(
        # StorageLevel.MEMORY_ONLY # stores the DF/RDD on the JVM as SERIALIZED objects (uses more memory, but very quick)
        # StorageLevel.MEMORY_AND_DISK # stores the DF/RDD on the JVM as SERIALIZED objects; if not possible in RAM, store on disk
        # StorageLevel.DISK_ONLY # stores just to disk (slower, but more spacious)
        # StorageLevel.MEMORY_ONLY_2 # memory only with 2x replication (for fault tolerance)
        # StorageLevel.DISK_ONLY_2 # same for disk (also 3x replication with _3), same for MEMORY_AND_DISK
        # cache() will call persist(StorageLevel.MEMORY_AND_DISK_DESER) # store on memory and disk as DESERIALIZED (raw) objects
        # StorageLevel.OFF_HEAP # stores in memory outside the JVM (serialization is done with Tungsten, quite fast)
    )

    # simulate some other computations on top of the expensive one
    df2 = highest_paid_df.selectExpr("_c1 as first_name", "_c3 as last_name", "salary")
    df3 = highest_paid_df.filter("salary > 50000")

    """
        df1
        df1.cache
        df2 = something(df1)
        df3 = df1.filter(...)
       
        Filter pushdown is prevented!
    """

    df2.explain()
    df3.explain()

    # final_df.show() # ~3s
    # sleep(5)
    # final_df.show() # 21ms - will re-evaluate the highest_paid_df which does another shuffle
    # people_df.printSchema()

    # remove things from cache
    highest_paid_df.unpersist() # cannot call uncache()

    # change cache name in the Spark UI
    highest_paid_df.createOrReplaceTempView("highest_paid_df")
    spark.catalog.cacheTable("highest_paid_df")

    # RDDs and DFs have the exact same cache mechanics


def demo_checkpoint():
    people_df = spark.read.option("sep", ":").csv("../data/people-1m")\
        .withColumnRenamed("_c6", "salary") \
        .withColumnRenamed("_c1", "first_name")\
        .withColumnRenamed("_c3", "last_name")

    # simulate something expensive
    highest_paid_df = people_df.orderBy(desc("salary"))

    # checkpoint: forget the query plan/lineage and will store to disk
    # used to avoid failure

    # set checkpoint directory (needs to be configured)
    sc.setCheckpointDir("../checkpoints")

    df2 = highest_paid_df.selectExpr("first_name", "last_name", "salary")
    df2.explain()
    # checkpointing returns a new DF with disk storage and zero lineage
    hpc = highest_paid_df.checkpoint()
    df3 = hpc.selectExpr("first_name", "last_name", "salary")
    df3.explain()



if __name__ == '__main__':
    demo_checkpoint()
    sleep(1000)
