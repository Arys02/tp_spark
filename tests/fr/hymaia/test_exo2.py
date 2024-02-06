from src.fr.hymaia.exo2.spark_clean_job import code_post_to_dep
from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo1.main import wordcount
from pyspark.sql import Row


class TestMain(unittest.TestCase):
    def test_aggregate(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(text='bonjour je suis un test unitaire'),
                Row(text='bonjour suis test')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(word='bonjour', count=2),
                Row(word='je', count=1),
                Row(word='suis', count=2),
                Row(word='un', count=1),
                Row(word='test', count=2),
                Row(word='unitaire', count=1),
            ]
        )

        actual = wordcount(input, 'text')

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_code_post_to_dep(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(zip='75017'),
                Row(zip='31000'),
                Row(zip='20233'),
                Row(zip='20100'),
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(zip='75017', dep='75'),
                Row(zip='31000', dep='31'),
                Row(zip='20233', dep='2A'),
                Row(zip='20100', dep='2B'),
            ]
        )

        actual = input.withColumn('dep', code_post_to_dep(input['zip']))

        self.assertCountEqual(actual.collect(), expected.collect())

