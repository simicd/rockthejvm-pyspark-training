
# install pandas, pyarrow
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .appName("Arrow Demo") \
    .getOrCreate()


def demo_arrow():
    # create a Pandas DF
    pdf = pd.DataFrame(np.random.rand(100000, 3))

    # convert a Pandas DF to a Spark DF
    df = spark.createDataFrame(pdf)
    df.show()


if __name__ == '__main__':
    print(datetime.now())
    demo_arrow()
    print(datetime.now()) # 11s (without Arrow), 5.5s (with Arrow)
    # 1000000x3: 40s (without Arrow), 6.5s (with Arrow)
