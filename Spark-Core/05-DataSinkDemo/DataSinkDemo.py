from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("DataFrameWrite API Demo") \
        .getOrCreate()
    logger = Log4J(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("datasource/flight*.parquet")

    flightTimeParquetDF.write.format("avro") \
        .mode("overwrite") \
        .option("path","datasink/avro/") \
        .save()

    # To get no of partitions, even though we have 2 partitions, we'll get 1 o/p record
    # because our 2nd partition doest have data
    logger.info("No Of Partitions Before: "+ str(flightTimeParquetDF.rdd.getNumPartitions()))

    # To get partition id, here we have only one partition, 2nd one is empty
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    print("*****REPARTITIONED FILE*****")
    '''
    partitionedDF = flightTimeParquetDF.repartition(4)
    logger.info("No Of Partitions Before: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    partitionedDF.write.format("avro").mode("overwrite").option("path","datasink/partitioned_avro").save()
    '''
    # To get Partitions based on cols & Max records per file
    flightTimeParquetDF.write.format("json").mode("overwrite").option("path", "datasink/partBy_json") \
        .partitionBy("OP_CARRIER", "ORIGIN")  \
        .option("maxRecordsPerFile", 10000) \
        .save()

    spark.stop()