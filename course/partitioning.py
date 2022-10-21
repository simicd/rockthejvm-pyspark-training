from pyspark.sql import SparkSession
from time import sleep


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Partitioning") \
    .getOrCreate()

sc = spark.sparkContext


def process_numbers(n_partitions):
    numbers = spark.range(1000000000, numPartitions=n_partitions) # 1B integers = 8GB in total
    # simulate something expensive on top of this DF
    numbers.selectExpr("sum(id)").show()


# ~1GB uncompressed data for optimal perf
def demo_partition_sizes():
    process_numbers(1) # 8GB/partition - 1s
    process_numbers(8) # 1GB/partition - 0.3s
    process_numbers(12) # 0.75GB/partition - 0.2s
    process_numbers(80) # 100MB/partition - 0.6s
    process_numbers(800) # 10MB/partition - 2.2s
    process_numbers(8000) # 1MB/partition - 16s
    process_numbers(80000) # 100kB/partition - around 3mins

# how to find how big your DF is:
#   - cache it, and look in the Storage tab to see how big it was
#   - cache a fraction of the data (via sampling) and then extrapolate


# repartition and coalesce
def demo_repartition_coalesce():
    numbers = sc.parallelize(range(1, 100000000))
    print(numbers.getNumPartitions())

    # repartition = a COMPLETE shuffle
    repartitioned_numbers = numbers.repartition(2)
    print(repartitioned_numbers.count()) # force the evaluation of the RDD ~1min

    # coalesce = to reduce the number of partitions
    coalesced_numbers = numbers.coalesce(2)
    # coalesce(n, True) == repartition(n)
    print(coalesced_numbers.count()) # 8s


def add_columns(df, n):
    new_columns = ["id * " + str(i) + " as newCol" + str(i) for i in range(n)]
    return df.selectExpr("id", *new_columns)


# df.selectExpr(id, id * 1 as newCol1, id * 2 as newCol2 ....)

def demo_pre_partitioning():
    initial_table = spark.range(1, 10000000).repartition(10)
    smaller_table = spark.range(1, 5000000).repartition(7)

    wide_table = add_columns(initial_table, 30)
    joined = wide_table.join(smaller_table, "id")
    # joined.explain()
    # joined.show() # ~14s

    # option 1: partition by id
    wide_by_id = wide_table.repartition("id") # spark.sql.shuffle.partitions (200) function = id % 200
    small_by_id = smaller_table.repartition("id") # function = id % 200
    joined_v2 = wide_by_id.join(small_by_id, "id")
    # joined_v2.explain()
    # joined_v2.show() # ~10s

    # option 2: sort the bigger one
    wide_sorted = wide_table.orderBy("id")
    joined_v3 = wide_sorted.join(smaller_table, "id")
    # joined_v3.explain()
    # joined_v3.show() # 14s, same as the first

    # option 3: do the column addition AFTER the join
    joined_v4 = add_columns(initial_table.join(smaller_table, "id"), 30)
    # joined_v4.explain()
    # joined_v4.show() # ~10s

    # option 4: force a shuffled hash join
    # spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
    joined_v5 = wide_by_id.hint("shuffle_hash").join(small_by_id, "id")
    # joined_v5.explain()
    # joined_v5.show() # ~14s

    # option 5: repartition by id BEFORE widening the original table
    initial_by_id = initial_table.repartition("id") # hash partitioner
    smaller_by_id = smaller_table.repartition("id")
    joined_v6 = add_columns(wide_by_id.join(smaller_by_id, "id"), 30)
    # joined_v6.explain()
    # joined_v6.show() # ~6s

    # option 6:
    initial_by_range = initial_table.repartitionByRange(12, "id")
    smaller_by_range = smaller_table.repartitionByRange(12, "id")
    wide_by_range = add_columns(initial_by_range, 30)
    joined_v7 = add_columns(initial_by_range.join(smaller_by_range, "id"), 30) # ANOTHER SHUFFLE!
    # joined_v7.explain()
    # joined_v7.show() # 5s

    # takeaway: make sure you shuffle as little data as possible
    # repartition early (if data becomes bigger), or late (if the data becomes smaller)

    # repartitionAndSortWithinPartitions
    # option OPTIMUS PRIME = option 5 + hint + add columns later
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
    initial_by_id = initial_table.repartition("id")
    smaller_by_id = smaller_table.repartition("id")
    joined_optimus = add_columns(initial_by_id.hint("shuffle_hash").join(smaller_by_id, "id"), 30)
    # joined_optimus.explain()
    # joined_optimus.show() # 3s


if __name__ == '__main__':
    demo_pre_partitioning()
    sleep(3600)