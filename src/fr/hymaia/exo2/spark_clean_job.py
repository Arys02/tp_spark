import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()

    city_zipcode = "src/resources/exo2/city_zipcode.csv"
    client_bdd = "src/resources/exo2/clients_bdd.csv"

    output = "data/exo2/output"

    # Création d'un DataFrame à partir d'un fichier CSV
    df_client = spark.read.csv(client_bdd, header=True, inferSchema=True)
    df_zipcode = spark.read.csv(city_zipcode, header=True, inferSchema=True)

    df_client_major = df_client.where(f.col("age") >= 18)
    df_client_major.show()

    df_joinded = df_client.join(df_zipcode, df_client_major.zip == df_zipcode.zip).drop(df_zipcode.zip)
    df_joinded.show()

    departement_udf = f.udf(lambda m: code_post_to_dep(m))

    dd = df_joinded.withColumn("departement", departement_udf(f.col("zip")))

    wordwrite(dd, output)


def code_post_to_dep(code_postal):
    dep = str(code_postal)[:2]
    if dep == "20":
        dep = "2A" if int(str(code_postal)) <= 20190 else "2B"

    return dep


def wordwrite(df, dst):
    df.write.mode("overwrite").parquet(dst)

