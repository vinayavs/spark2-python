from pyspark.sql import *

from lib.logger import Log4J

if __name__ == '__main__':
    # Enabling hive metastore to persists managed table metadata
    spark = SparkSession.builder.master("local[2]").appName("Managed Tables Demo") \
        .enableHiveSupport().getOrCreate()
    logger = Log4J(spark)

    flightTimeParquetDF = spark.read.format("parquet").load("datasource/")

    # Save managed table in specific db in 2 ways
    spark.sql("CREATE DATABASE IF NOT EXISTS flightdb")
    # flightTimeParquetDF.write.mode("overwrite").saveAsTable("flightdb.flight_data_tbl")
    spark.catalog.setCurrentDatabase("flightdb")
    # flightTimeParquetDF.write.mode("overwrite").saveAsTable("flight_data_tbl")
    flightTimeParquetDF.write.mode("overwrite") \
        .partitionBy("ORIGIN","OP_CARRIER") \
        .saveAsTable("flight_data_tbl")

    # flightTimeParquetDF.write.format("csv").mode("overwrite") \
    #     .bucketBy(4,"ORIGIN","OP_CARRIER") \
    #     .sortBy("ORIGIN","OP_CARRIER")
    #     .saveAsTable("flight_data_tbl")

    print("*********SHOW TABLES**********")
    logger.info(spark.catalog.listTables("flightdb"))


