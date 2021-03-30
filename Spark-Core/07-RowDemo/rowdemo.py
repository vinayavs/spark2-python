from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4J

def to_date_df(df, fmt, col):
    return df.withColumn(col, to_date(col, fmt))

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[2]").appName("RowDemo").getOrCreate()

    logger = Log4J(spark)

    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())
    ])

    # Each row in a DF is a single record represented by an object of type Row
    my_rows = [Row("12", "04/05/2020"), Row("13", "4/5/2020"), Row("14", "12/10/2020")]
    # my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
    my_rdd = spark.sparkContext.parallelize(my_rows,2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    my_df.printSchema()
    my_df.show()

    new_df = to_date_df(my_df, 'M/d/y', 'EventDate')
    new_df.printSchema()
    new_df.show()

    spark.stop()









