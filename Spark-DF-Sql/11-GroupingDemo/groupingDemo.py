from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[2]").appName("Grouping Demo") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()
    logger = Log4J(spark)

    summaryDf = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    # summaryDf.printSchema()

    numInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    totalQuantity = f.sum("Quantity").alias("TotalQuantity")
    invoiceValue = f.expr("round(sum(Quantity * UnitPrice), 2) as InvoiceValue")

    exSummaryDf = summaryDf \
        .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
        .groupBy("Country","WeekNumber") \
        .agg(numInvoices, totalQuantity, invoiceValue)

    exSummaryDf.coalesce(1) \
        .write.format("parquet").mode("overwrite") \
        .save("output")
    exSummaryDf.sort("Country", "WeekNumber").show()
