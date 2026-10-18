import os

from pyspark.sql import SparkSession

os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {os.environ['SPARK_PACKAGES']} pyspark-shell"
SparkSession.builder.master("local[1]").getOrCreate().stop()
