from pyspark.sql import SparkSession
from pyspark.sql.types import *

from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("File Formats Demo") \
        .getOrCreate()
    logger = Log4J(spark)

    # Defining Explict Schema Programmatically
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    # Defining Explicit schema using DDL stmt.
    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    flightTimeCsvDF = spark.read \
        .format('csv') \
        .option('header', 'true') \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load('data/flight-time.csv')
    # with inferSchema - we'll get orginal datatypes, without it - everything will be string, But Date filed is still sting
    # for this need to use explict/Implict(for avro, parquet..) schema
    flightTimeCsvDF.show(5)
    logger.info("CSV Schema: " + flightTimeCsvDF.schema.simpleString())

    flightTimeJsonDF = spark.read \
        .format('json') \
        .schema(flightSchemaDDL) \
        .option("dateFormat", "M/d/y") \
        .load('data/flight*.json')

    flightTimeJsonDF.show(5)
    logger.info("JSON Schema: " + flightTimeJsonDF.schema.simpleString())


    flightTimeParqDF = spark.read \
        .format('parquet') \
        .load('data/flight*.parquet')
    flightTimeJsonDF.show(5)
    logger.info("PARQUET Schema: " + flightTimeParqDF.schema.simpleString())

    spark.stop()