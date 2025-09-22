from spark_utils import get_spark
spark = get_spark("ins_data_analysis")

event_silver = spark.read.parquet("/Users/pd_10/Desktop/gcs_data/silver/event")

event_silver.createOrReplaceTempView("event_silver")



# Total number of events
total_events=spark.sql(f'''SELECT COUNT(*) AS total_events from event_silver''')

print('Total number of events:')
total_events.show()

#Distribution of Events by Type
events_type=spark.sql(f'''SELECT eventname, COUNT(*) AS event_count
FROM event_silver
GROUP BY eventname
ORDER BY event_count DESC''')

print('Distribution of Events by Type:')
events_type.show()

#Daily Active Users
dau=spark.sql(f'''SELECT DATE(event_ts) AS day, COUNT(DISTINCT deviceid) AS dau
FROM event_silver
GROUP BY day
ORDER BY day''')

print('Daily Active Users:')
dau.show()

#Weekly Active Users
wau=spark.sql(f'''SELECT YEAR(event_ts) AS year, WEEKOFYEAR(event_ts) AS week,
       COUNT(DISTINCT deviceid) AS wau
FROM event_silver
GROUP BY year, week
ORDER BY year, week''')

print('Weekly Active Users:')
wau.show()

#Monthly Active Users
mau=spark.sql(f'''SELECT YEAR(event_ts) AS year, MONTH(event_ts) AS month,
       COUNT(DISTINCT deviceid) AS mau
FROM event_silver
GROUP BY year, month
ORDER BY year, month''')

print('Monthly Active Users:')
mau.show()

#Average Timespent per Event Type

avg_timespent=spark.sql(f'''SELECT eventname,
       AVG(timespent) AS avg_time,
       MAX(timespent) AS max_time
FROM event_silver
GROUP BY eventname''')

print('Average Timespent per Event Type:')
avg_timespent.show()

#Outlier Detection in Timespent

outlier=spark.sql(f"""SELECT COUNT(*) AS outliers
FROM event_silver
WHERE timespent > 10800""")

print('Outlier Detection in Timespent (events with timespent > 3 hours):')
outlier.show()

# Top 10 Active Users by Event Count and Total Timespent
most_active_users=spark.sql('''SELECT deviceid, COUNT(*) AS total_events, SUM(timespent) AS total_time
FROM event_silver
GROUP BY deviceid
ORDER BY total_events DESC
LIMIT 10''')

print('Top 10 Active Users by Event Count and Total Timespent:')
most_active_users.show()