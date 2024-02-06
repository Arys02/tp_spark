import pyspark.sql.functions as f

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()


def wordwrite(df, dst):
    df.write.mode("overwrite").partitionBy("count").parquet(dst)

