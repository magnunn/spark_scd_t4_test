import findspark
findspark.init()

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import re
from datetime import datetime


def get_file_date(file):
    regex_file_date = 'num_(.+)\.'
    r = re.search(regex_file_date, file).group(1)

    return r


def update_bronze(file, file_date):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("PySpark read csv") \
        .getOrCreate()

    players_num_df = spark.read.csv(file, header=True, sep=";")
    players_num_df = players_num_df.withColumn('timestamp', sf.lit(file_date))
    #players_num_df.write.parquet("bronze_t4\players_num.parquet")


    return players_num_df


file = "landing\players_num_221011 103116.csv"

file_date = get_file_date(file)

file_date_ajusted = datetime.strptime(file_date, r'%y%m%d %H%M%S')
df = update_bronze(file, file_date_ajusted)
df.show()

df.write.csv("bronze_t4\players_num.csv", mode='overwrite')


df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("example2")
df.repartition(1).write.csv("bronze_t4\players_num.csv", mode='overwrite')
