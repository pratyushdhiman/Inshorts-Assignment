from spark_utils import get_spark
from pyspark.sql.functions import col
spark = get_spark("ins_data_analysis")

event_silver = spark.read.parquet("/Users/pd_10/Desktop/gcs_data/silver/event")
user_silver = spark.read.parquet("/Users/pd_10/Desktop/gcs_data/silver/user")
content_silver = spark.read.parquet("/Users/pd_10/Desktop/gcs_data/silver/content")

combined_events = event_silver.alias("e") \
    .join(user_silver.alias("u"), col("e.deviceid") == col("u.deviceid"), "left") \
    .join(content_silver.alias("c"), col("e.content_id") == col("c._id"), "left") \
    .select(
        col("e.deviceid"),
        col("u.lang").alias("user_lang"),
        col("u.district"),
        col("u.platform"),
        col("u.install_dt"),
        col("u.campaign_id"),
        col("e.content_id"),
        col("e.eventname"),
        col("e.event_ts"),
        col("e.timespent"),
        col("c.newsLanguage").alias("content_lang"),
        col("c.categories_array"),
        col("c.author")
    )

combined_events.createOrReplaceTempView("gold_events")

#Funnel Performance by User Segments

funnel_performance=spark.sql(f'''SELECT platform, eventname, COUNT(*) AS events
FROM gold_events
GROUP BY platform, eventname
ORDER BY platform, events DESC''')

print('Funnel Performance by User Segments:')
funnel_performance.show()

#Engagement by Content Category

engagement=spark.sql('''SELECT category, COUNT(*) AS interactions, ROUND(AVG(timespent),2) AS avg_time
FROM gold_events
LATERAL VIEW explode(categories_array) t AS category
GROUP BY category
ORDER BY interactions DESC''')

print('Engagement by Content Category:')
engagement.show()

#Campaign Effectiveness Beyond Installs

campaign_effectiveness=spark.sql(f'''SELECT campaign_id, COUNT(DISTINCT deviceid) AS users,
       COUNT(*) AS total_events,
       ROUND(AVG(timespent),2) AS avg_timespent
FROM gold_events
GROUP BY campaign_id
ORDER BY users DESC''')

print('Campaign Effectiveness Beyond Installs:')
campaign_effectiveness.show()

#Retention Analysis (D1, W1, M1)

##daily retention

daily_retention=spark.sql(f'''WITH installs AS (
  SELECT deviceid, install_dt
  FROM gold_events
  GROUP BY deviceid, install_dt
),
day1 AS (
  SELECT DISTINCT g.deviceid
  FROM gold_events g
  JOIN installs i ON g.deviceid = i.deviceid
  WHERE DATE(g.event_ts) = DATE_ADD(i.install_dt, 1)
)
SELECT COUNT(d.deviceid)*1.0 / COUNT(i.deviceid) AS d1_retention
FROM installs i
LEFT JOIN day1 d ON i.deviceid = d.deviceid''')

print('D1 Retention:')
daily_retention.show()

##weekly retention

weekly_retention=spark.sql(f'''WITH installs AS (
  SELECT deviceid, install_dt
  FROM gold_events
  GROUP BY deviceid, install_dt
),
week1 AS (
  SELECT DISTINCT g.deviceid
  FROM gold_events g
  JOIN installs i ON g.deviceid = i.deviceid
  WHERE DATE(g.event_ts) BETWEEN DATE_ADD(i.install_dt, 1) AND DATE_ADD(i.install_dt, 7)
)
SELECT COUNT(w.deviceid)*1.0 / COUNT(i.deviceid) AS w1_retention
FROM installs i
LEFT JOIN week1 w ON i.deviceid = w.deviceid''')

print('W1 Retention:')
weekly_retention.show()

##monthly retention

monthly_retention=spark.sql(f'''WITH installs AS (
  SELECT deviceid, install_dt
  FROM gold_events
  GROUP BY deviceid, install_dt
),
month1 AS (
  SELECT DISTINCT g.deviceid
  FROM gold_events g
  JOIN installs i ON g.deviceid = i.deviceid
  WHERE DATE(g.event_ts) BETWEEN DATE_ADD(i.install_dt, 1) AND DATE_ADD(i.install_dt, 30)
)
SELECT COUNT(m.deviceid)*1.0 / COUNT(i.deviceid) AS m1_retention
FROM installs i
LEFT JOIN month1 m ON i.deviceid = m.deviceid''')

print('M1 Retention:')
monthly_retention.show()

#Churn Analysis

####Week-to-Week

churn_analysis_weekly=spark.sql(f'''WITH weekly AS (
  SELECT deviceid, WEEKOFYEAR(event_ts) AS wk
  FROM gold_events
  GROUP BY deviceid, WEEKOFYEAR(event_ts)
)
SELECT w1.wk,
       COUNT(DISTINCT w1.deviceid) AS active_users,
       COUNT(DISTINCT w1.deviceid) - COUNT(DISTINCT w2.deviceid) AS churned
FROM weekly w1
LEFT JOIN weekly w2
  ON w1.deviceid = w2.deviceid AND w2.wk = w1.wk + 1
GROUP BY w1.wk
ORDER BY w1.wk''')

print('Churn Analysis (Week-to-Week):')
churn_analysis_weekly.show()

###category-level churn
category_churn=spark.sql('''WITH user_category_week AS (
    SELECT deviceid,
           WEEKOFYEAR(event_ts) AS wk,
           category,
           COUNT(*) AS interactions
    FROM gold_events
    LATERAL VIEW explode(categories_array) t AS category
    GROUP BY deviceid, WEEKOFYEAR(event_ts), category
),
dominant_category AS (
    SELECT deviceid, wk, category
    FROM (
        SELECT deviceid, wk, category,
               ROW_NUMBER() OVER (PARTITION BY deviceid, wk ORDER BY interactions DESC) AS rnk
        FROM user_category_week
    ) ranked
    WHERE rnk = 1
),
category_churn AS (
    SELECT this.deviceid,
           this.wk AS current_week,
           this.category AS current_category,
           next.category AS next_category
    FROM dominant_category this
    LEFT JOIN dominant_category next
           ON this.deviceid = next.deviceid
          AND next.wk = this.wk + 1
)
SELECT current_category,
       COUNT(*) AS users,
       SUM(CASE WHEN next_category IS NULL THEN 1 ELSE 0 END) AS churned_out,
       SUM(CASE WHEN next_category IS NOT NULL AND next_category != current_category THEN 1 ELSE 0 END) AS switched,
       SUM(CASE WHEN next_category = current_category THEN 1 ELSE 0 END) AS retained
FROM category_churn
GROUP BY current_category
ORDER BY users DESC''')

print('Category-Level Churn Analysis:')
category_churn.show()