from unittest import TestCase

from pyspark.sql.types import Row

from clustering import get_clusters


class TestCluster(TestCase):
  def test_get_clusters(self):
    d = {
      "latitude": [-19.5171, -19.5155, -19.5143, -19.0982, -19.0873],
      "longitude": [-63.5559, -63.5333, -63.5268, -63.758, -63.763],
    }

    clusters = get_clusters(row=Row(**d))
    self.assertEqual("", clusters)
