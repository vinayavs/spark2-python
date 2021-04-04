from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[2]").appName("Aggregations Demo") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()
    logger = Log4J(spark)

    invoiceDf = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")


    # invoiceDf.printSchema()

    # Column Object Expression
    invoiceDf.select(f.count("*").alias("Count *"),
                     f.sum("Quantity").alias("TotalQuantity"),
                     f.avg("UnitPrice").alias("AvgPrice"),
                     f.countDistinct("InvoiceNo").alias("CountDistinct")).show()

    # Using Sql like string Expression
    invoiceDf.selectExpr(
        "count(1) as RecordCount",  # Includes Null
        "count(StockCode) as TickerCount", # Ignores Null
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as UnitPrice"
    ).show()

    # Grouping Aggregations
    invoiceDf.createOrReplaceTempView("sales")
    summarySql = spark.sql("""
    SELECT Country, InvoiceNo,
           sum(Quantity) as TotalQuantity,
           round(sum(Quantity * UnitPrice), 2) as InvoiceValue 
    FROM sales
    GROUP BY Country, InvoiceNo
    """)
    summarySql.show()

    # DF Expressions
    summaryDf = invoiceDf \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.expr("round(sum(Quantity * UnitPrice), 2) as InvoiceValue"))
    # f.round(f.sum(f.expr("Quantity * UnitPrice")),2).alias("InvoiceValue")
    summaryDf.show()
