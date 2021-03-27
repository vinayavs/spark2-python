from pyspark.sql import *


if __name__ == '__main__':
    # print("Hello PySpark..!!")
    spark = SparkSession().Builder \
        .appName("Hello Spark") \
        .master("Local[3]") \
        .getOrCreate()

    # logger = Log4J(spark)
    spark.stop()