from src.fr.hymaia.exo2.spark_clean_job import code_post_to_dep, clean
from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo1.main import wordcount
from pyspark.sql import Row


class TestMain(unittest.TestCase):
    def test_clean(self):
        # GIVEN
        input_zip = spark.createDataFrame(
            [
                Row(zip='75017', city='paris'),
                Row(zip='31000', city='toulouse'),
                Row(zip='20233', city='corse1'),
                Row(zip='20100', city='corse2'),
            ]
        )
        input_client = spark.createDataFrame(
            [
                Row(zip='75017', age=67, name='toto'),
                Row(zip='31000', age=46, name='tata'),
                Row(zip='20233', age=25, name='titi'),
                Row(zip='20100', age=54, name='tutu'),
                Row(zip='20105', age=12, name='petit'),
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(zip='75017', age=67, name='toto', city='paris', departement='75'),
                Row(zip='31000', age=46, name='tata', city='toulouse', departement='31'),
                Row(zip='20233', age=25, name='titi', city='corse1', departement='2B'),
                Row(zip='20100', age=54, name='tutu', city='corse2', departement='2A'),
            ]
        )

        actual = clean(input_client, input_zip)

        self.assertCountEqual(actual.collect(), expected.collect())
