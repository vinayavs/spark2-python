from datetime import date
from unittest import TestCase

from pyspark.sql import *
from pyspark.sql.types import *
from rowdemo import to_date_df


class RowDemoTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.master("local[2]").appName("Row Demo Test") \
            .getOrCreate()

        my_schema = StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())
        ])

        my_rows = [Row("12", "04/05/2020"), Row("13", "4/5/2020"), Row("14", "04/5/2020")]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows,2)
        cls.my_df = cls.spark.createDataFrame(my_rows, my_schema)

    # Validating the datatype
    def test_data_type(self):
        rows = to_date_df(self.my_df, 'M/d/y', 'EventDate').collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)


    def test_date_value(self):
        rows = to_date_df(self.my_df, 'M/d/y', 'EventDate').collect()
        for row in rows:
            self.assertEqual(row["EventDate"], date(2020, 4, 5))
