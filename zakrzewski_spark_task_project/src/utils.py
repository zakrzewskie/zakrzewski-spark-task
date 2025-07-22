from pyspark.sql import SparkSession

def get_spark_session(execution_mode):
    if execution_mode == "local":
        spark = SparkSession.builder \
            .appName("LocalSparkPipeline") \
            .master("local[*]") \
            .getOrCreate()
    else:
        spark = SparkSession.builder.getOrCreate()

    return spark