from spark_utils import get_spark
from pyspark.sql.functions import from_unixtime, col

spark = get_spark("ins_data_analysis")

base_path = "/Users/pd_10/Desktop/gcs_data"

event_df = spark.read.option("header", "true").csv(f"{base_path}/Event/")

#convert data types
event_silver = event_df.withColumn("event_ts", from_unixtime(col("eventtimestamp").cast("bigint")/1000))
event_silver = event_silver.withColumn("timespent", col("timespent").cast("double"))

#Validation of event names

valid_events = ["Opened", "Shown", "Front", "Back", "Shared"]
event_silver = event_silver.filter(col("eventname").isin(valid_events))

#dropping null values from deviceid and content_id as these are key columns

event_silver = event_silver.dropna(subset=["deviceid", "content_id"])

#filling null time spent rows with 0

event_silver = event_silver.fillna({"timespent": 0})
#dropping duplicate rows

event_silver = event_silver.dropDuplicates(["deviceid", "content_id", "eventtimestamp", "eventname"])

event_silver.repartition(5).write.mode("overwrite").parquet("/Users/pd_10/Desktop/gcs_data/silver/event")
