from pyspark.sql import SparkSession

from src.fr.hymaia.exo1.spark_wordcount import wordcount, wordwrite


def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()

    csv_file = "src/resources/exo1/data.csv"
    output = "data/exo1/output"

    df = spark.read.csv(csv_file, header=True)
    w = wordcount(df, "text")

    wordwrite(w, output)

