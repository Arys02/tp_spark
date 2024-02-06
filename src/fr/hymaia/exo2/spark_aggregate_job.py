import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()

    input = "data/exo2/output"
    output = "data/exo2/output/aggregate.csv"

    df_client = spark.read.parquet(input, header=True, inferSchema=True)
    df_client.show()

    df_dep_count = aggregate(df_client)
    df_dep_count.show()

    aggregate_save(df_dep_count, output)


def aggregate(df):
    return df.groupBy("departement").count().withColumnRenamed("count", "nb_people")


def aggregate_save(df, output):
    df.coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header", "true").save(
        output)
