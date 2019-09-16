from pyspark import SparkConf
from pyspark.sql import SparkSession, functions

from clustering import get_clusters_for_partition

base_data_dir = "/Users/timor/projects/data/amazonlab/nasa_fire_info/BOLIVIA_DL_FIRE_M6_5242/"
csvInPath = base_data_dir + "fire_archive_M6_5242.csv"
parquetOutPath = base_data_dir + "fire_archive_M6_5242_clustered_eps005_s4.parquet"

spark_conf = SparkConf()
spark_conf.set('spark.executor.memory', '4g')
spark_conf.setAppName("furry-lab")
spark_conf.setMaster("local[*]")
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

csv_options = {"delimiter": ",", "header": "true", "inferSchema": "true"}
# add grouping week with year
ds = spark.read.format("csv").options(**csv_options).load(csvInPath) \
  .withColumn("week", functions.concat(functions.year("acq_date"), functions.lit("_"), functions.weekofyear("acq_date")))

week_ds = ds.groupBy("week").agg(
    functions.collect_list(functions.struct(
        "latitude",
        "longitude",
        "brightness",
        "acq_date",
        "confidence",
        "bright_t31",
        "week"
    )).alias("events")
).sort("week")

clustered_ds = week_ds.rdd.mapPartitions(get_clusters_for_partition).toDF().cache()
clustered_ds.count()
clustered_ds.coalesce(1).write.parquet(parquetOutPath)
