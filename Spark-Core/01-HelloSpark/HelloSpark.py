import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == '__main__':
    # print("Hello PySpark..!!")
    # conf = SparkConf()
    # conf.set("spark.app.name","Hello Spark")
    # conf.set("spark.master","local[2]")

    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) !=2:
        logger.error("Usage: Hello Spark <File Name>")
        sys.exit(-1)

    logger.info("******STARTING SPARK JOB******")
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    survey_df = load_survey_df(spark,sys.argv[1])
    # survey_df.show()
    # Increasing the partitions to test wide transformations
    partitioned_df = survey_df.repartition(2)

    count_df = count_by_country(partitioned_df)
    # count_df.show()
    logger.info(count_df.collect())

    input("Press Enter")  # For Local debugging purpose only

    logger.info("******SPARK JOB FINISHED******")
    # spark.stop()