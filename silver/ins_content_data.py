from spark_utils import get_spark
from pyspark.sql.functions import from_unixtime, col, upper, split

spark = get_spark("ins_data_analysis")

base_path = "/Users/pd_10/Desktop/gcs_data"

content_df = spark.read.option("header", "true").csv(f"{base_path}/Content/")

#dropping null _id as it is key column
content_silver = content_df.dropna(subset=["_id"])

#dropping duplicate _id as it is key column
content_df=content_df.dropDuplicates(["_id"])

#casting createdAt to timestamp
content_silver=content_silver.withColumn("createdAt", col("createdAt").cast("timestamp"))

#splitting the categories column
content_silver = content_df.withColumn("categories_array",split(col("categories"), "\\|"))

#replaceing null author values to "Unknown"
content_silver=content_silver.fillna({"author": "Unknown",'newsLanguage': "other"})

#uppercaseing newsLanguage column
content_silver=content_silver.withColumn('newsLanguage', upper(col('newsLanguage')))

content_silver.write.mode("overwrite").parquet("/Users/pd_10/Desktop/gcs_data/silver/content")
