from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[2]").appName("Windowing Aggregations") \
        .config("spark.sql.shuffle.partitions", 2).getOrCreate()
    logger = Log4J(spark)

    summaryDf = spark.read.parquet("data/summary.parquet")

    # summaryDf.printSchema()

    runningTotalWindow = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # To get running total 3 rows before current row, so last 4 weeks
    # runningTotalWindow = Window.partitionBy("Country") \
    #     .orderBy("WeekNumber") \
    #     .rowsBetween(-3, Window.currentRow)

    summaryDf.withColumn("RunningTotal", f.sum("InvoiceValue").over(runningTotalWindow)).show()