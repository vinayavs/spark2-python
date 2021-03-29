import sys
from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from lib.logger import Log4J

surveySchema = namedtuple("SurveySchema",["Age","Gender","Country","State"])

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("Hello RDD")
    # sc = SparkContext(conf=conf)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloRDD <File Name>")
        sys.exit(-1)

    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)

    colsRDD = partitionedRDD.map(lambda line: line.replace('"','').split(','))
    selectRDD = colsRDD.map(lambda cols: surveySchema(int(cols[1]), cols[2], cols[3], cols[4]))
    filterRDD = selectRDD.filter(lambda s: s.Age < 40)
    kvRDD = filterRDD.map(lambda s: (s.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1,v2 : v1+v2 )

    colsList = countRDD.collect()
    for x in colsList:
        logger.info(x)


