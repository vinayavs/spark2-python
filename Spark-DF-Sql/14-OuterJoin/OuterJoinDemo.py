from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[2]").appName("Joins Demo") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()

    logger = Log4J(spark)

    orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

    orderDf = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")
    product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]
    productDf = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")

    orderDf.show()
    productDf.show()

    join_expr = orderDf.prod_id == productDf.prod_id
    productRenamedDf = productDf.withColumnRenamed("qty", "reorder_qty")

    orderDf.join(productRenamedDf, join_expr, "outer") \
        .drop(productRenamedDf.prod_id) \
        .select('*') \
        .sort("order_id") \
        .show()

    # coalesce() - will returns first non-null value of given column lsit
    orderDf.join(productRenamedDf, join_expr, "left") \
        .drop(productRenamedDf.prod_id) \
        .select("order_id","prod_id","prod_name","unit_price","list_price","qty") \
        .withColumn("list_price", expr("coalesce(list_price, unit_price)")) \
        .sort("order_id").show()

    orderDf.join(productRenamedDf, join_expr, "right") \
        .drop(orderDf.prod_id) \
        .select("prod_id","prod_name","unit_price","list_price","reorder_qty") \
        .withColumn("prod_name", expr("coalesce(prod_name, prod_id)")) \
        .withColumn("unit_price", expr("coalesce(unit_price, list_price)")) \
        .sort("order_id").show()




