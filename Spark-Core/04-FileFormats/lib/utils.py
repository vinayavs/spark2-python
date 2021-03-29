import configparser
import sys

from pyspark import SparkConf

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("app.props")

    for (key, value) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, value)
    return spark_conf

def load_survey_df(spark,data_file):
    return spark.read \
        .option("header", "true") \
        .option("InferSchema", "true") \
        .csv(data_file)
def count_by_country(df):
    return df.where("Age < 40") \
        .select("Age", "Gender", "Country", "State") \
        .groupBy("Country") \
        .count()