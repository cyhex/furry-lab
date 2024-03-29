import numpy as np
from pyspark.sql.types import Row
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import normalize


def get_clusters(row, min_samples=4):
  events = row.events
  X = np.zeros((len(events), 2))
  for i, event in enumerate(events):
    if type(event) is not dict:
      event = event.asDict
    X[i][0] = event['latitude']
    X[i][1] = event['longitude']
  X = normalize(X)
  dbscan = DBSCAN(eps=0.005, min_samples=min_samples)
  predict = dbscan.fit_predict(X)
  rows = []
  for i, event in enumerate(events):
    if type(event) is not dict:
      event = event.asDict

    event["cluster"] = int(predict[i])
    rows.append(Row(**event))

  return rows


def get_clusters_for_partition(partition):
  rows = []
  for row in partition:
    rows.extend(get_clusters(row))
  return iter(rows)
