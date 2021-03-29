import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("Hello Spark SQL") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: Hello Spark SQL <File Name>")
        sys.exit(-1)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[1])
    survey_df.createOrReplaceTempView("survey_view")
    count_df = spark.sql("select country, count(1) from survey_view where Age<40 group by Country")
    count_df.show()

    # input("Press enter")
    spark.stop()


