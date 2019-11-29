from pyspark.sql import SparkSession

spark = ( SparkSession
          .builder
          .appName("Python Spark SQL basic example")
          .getOrCreate())

print(spark.version)
