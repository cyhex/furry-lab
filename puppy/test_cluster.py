import json
from unittest import TestCase

from pyspark.sql.types import Row

from clustering import get_clusters


class TestCluster(TestCase):
  def test_get_clusters(self):
    data = json.load(open('../sample_data/week_group.json'))

    clusters = get_clusters(row=Row(**data), min_samples=2)
    self.assertEqual("", clusters)
