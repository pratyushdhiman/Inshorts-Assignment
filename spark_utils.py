from pyspark.sql import SparkSession

def get_spark(app_name="ins_data_analysis"):
    return SparkSession.builder\
        .appName(app_name)\
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
