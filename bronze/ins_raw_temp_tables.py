from spark_utils import get_spark
spark = get_spark("ins_data_analysis")

base_path = "/Users/pd_10/Desktop/gcs_data"

user_df = spark.read.option("header", "true").csv(f"{base_path}/User/")
event_df = spark.read.option("header", "true").csv(f"{base_path}/Event/")
content_df = spark.read.option("header", "true").csv(f"{base_path}/Content/")

# Register raw tables
user_df.createOrReplaceTempView("user_raw")
event_df.createOrReplaceTempView("event_raw")
content_df.createOrReplaceTempView("content_raw")


spark.sql('''select * from user_raw''').show(5)
spark.sql('''select * from event_raw''').show(5)
spark.sql('''select * from content_raw''').show(5)