from src.fr.hymaia.exo2.spark_aggregate_job import aggregate
from src.fr.hymaia.exo2.spark_clean_job import code_post_to_dep
from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo1.main import wordcount
from pyspark.sql import Row


class TestMain(unittest.TestCase):
    def test_aggregate(self):
        # GIVEN
        input= spark.createDataFrame(
            [
                Row(zip='75017', age=67, name='toto', city='paris', departement='75'),
                Row(zip='75017', age=67, name='toto', city='paris', departement='75'),
                Row(zip='75017', age=67, name='toto', city='paris', departement='75'),
                Row(zip='75017', age=67, name='toto', city='paris', departement='75'),
                Row(zip='75017', age=67, name='toto', city='paris', departement='75'),
                Row(zip='31000', age=46, name='tata', city='toulouse', departement='31'),
                Row(zip='31000', age=46, name='tata', city='toulouse', departement='31'),
                Row(zip='31000', age=46, name='tata', city='toulouse', departement='31'),
                Row(zip='20233', age=25, name='titi', city='corse1', departement='2B'),
            ]
        )

        expected = spark.createDataFrame(
            [
                Row(departement='75', nb_people=5),
                Row(departement='31', nb_people=3),
                Row(departement='2B', nb_people=1),
            ]
        )

        actual = aggregate(input)

        self.assertCountEqual(actual.collect(), expected.collect())
