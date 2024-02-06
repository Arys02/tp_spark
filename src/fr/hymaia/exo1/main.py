import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()

    csv_file = "src/resources/exo1/data.csv"
    output = "data/exo1/output"

    # Création d'un DataFrame à partir d'un fichier CSV
    df = spark.read.csv(csv_file, header=True)

    w = wordcount(df, "text")
    w.show()

    wordwrite(w, output)
    print("Hello world!")


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()


def wordwrite(df, dst):
    df.write.mode("overwrite").partitionBy("count").parquet(dst)
