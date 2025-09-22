from spark_utils import get_spark
from pyspark.sql.functions import from_unixtime, col, upper

spark = get_spark("ins_data_analysis")

base_path = "/Users/pd_10/Desktop/gcs_data"

user_df = spark.read.option("header", "true").csv(f"{base_path}/User/")

#dropping null users as it is key column
user_silver = user_df.dropna(subset=["deviceid"])

#uppercaseing platform column
user_silver=user_silver.withColumn("platform", upper(col("platform")))

#casting install_dt to date
user_silver=user_silver.withColumn("install_dt", col("install_dt").cast("date")) \

#dropping duplicate users

user_silver=user_silver.dropDuplicates(["deviceid"])


#updating null values in campaign_id to "organic"

user_silver=user_silver.fillna({"campaign_id": "organic",'district': "Unknown"})

user_silver.write.mode("overwrite").parquet("/Users/pd_10/Desktop/gcs_data/silver/user")

