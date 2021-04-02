from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder.appName("local[2]").appName("Miscellaneous Operations") \
        .getOrCreate()
    logger = Log4J(spark)

    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981

    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
    raw_df.printSchema()

    # Adding a new column
    df1 = raw_df.withColumn("id", monotonically_increasing_id())
    df1.show()

    # Inline Casting
    # df2 = df1.withColumn("year", expr("""
    #     case when year < 21 then cast(year as int) + 2000
    #     when year < 100 then cast(year as int) + 1900
    #     else year
    #     end
    #     """))

    # Changing the Schema
    # df2 = df1.withColumn("year", expr("""
    #     case when year < 21 then cast(year as int) + 2000
    #     when year < 100 then cast(year as int) + 1900
    #     else year
    #     end
    #     """).cast(IntegerType()))

    # Column Object Expression for case expression
    # df = df1.withColumn("day", col("day").cast(IntegerType())) \
    #     .withColumn("month", col("month").cast(IntegerType())) \
    #     .withColumn("year", col("year").cast(IntegerType()))
    # df2 = df.withColumn("year", \
    #                      when(col("year") < 20, col("year") + 2000) \
    #                      .when(col("year") < 100, col("year") + 1900) \
    #                      .otherwise(col("year")))
    #
    # df2.show()

    # Dropping Columns & duplicates
    # df3 = df2.withColumn("dob", expr("to_date(concat(year,'/', month,'/', day), 'y/M/d'))"))
    # df3 = df2.withColumn("dob", to_date(expr("concat(year,'/', month,'/', day)"),'y/M/d')) \
    #     .drop('day', 'month', 'year').dropDuplicates(["name","dob"]).sort(expr("dob desc"))
    # df3.show()

    final_df = raw_df.withColumn("id", monotonically_increasing_id()) \
        .withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("year", col("year").cast(IntegerType())) \
        .withColumn("year", when(col("year") < 20, col("year") + 2000)
                    .when(col("year") < 100, col("year") + 1900)
                    .otherwise(col("year"))) \
        .withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')")) \
        .drop("day", "month", "year") \
        .dropDuplicates(["name", "dob"]) \
        .sort(expr("dob desc"))

    final_df.show()

