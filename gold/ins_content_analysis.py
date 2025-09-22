from spark_utils import get_spark
spark = get_spark("ins_data_analysis")

content_silver = spark.read.parquet("/Users/pd_10/Desktop/gcs_data/silver/content")

content_silver.createOrReplaceTempView("content_silver")

# Total number of content items
content_items=spark.sql(f'''SELECT COUNT(DISTINCT _id) AS total_content FROM content_silver''')

print('Total number of content items:')
content_items.show()

# Content items over time
####Daily
daily_content=spark.sql(f'''SELECT DATE(createdAt) AS day, COUNT(DISTINCT _id) AS articles
FROM content_silver
GROUP BY day ORDER BY day''')

print('Content items daily:')
daily_content.show()

####monthly

monthly_content=spark.sql(f'''SELECT YEAR(createdAt) AS year, MONTH(createdAt) AS month,
       COUNT(DISTINCT _id) AS articles
FROM content_silver
GROUP BY year, month ORDER BY year, month''')

print('Content items monthly:')
monthly_content.show()

# Content distribution by Language
content_dist_lang=spark.sql(f'''SELECT newsLanguage, COUNT(DISTINCT _id) AS articles
FROM content_silver
GROUP BY newsLanguage ORDER BY articles DESC''')

print('Content distribution by Language:')
content_dist_lang.show()

# Content distribution by category
category_distribution=spark.sql(f'''SELECT category, COUNT(DISTINCT _id) AS articles
FROM content_silver
LATERAL VIEW explode(categories_array) t AS category
GROUP BY category
ORDER BY articles DESC''')

print('Content distribution by category:')
category_distribution.show()

# Top 10 authors by number of articles

author_distribution=spark.sql(f'''SELECT author, COUNT(DISTINCT _id) AS articles
FROM content_silver
GROUP BY author
ORDER BY articles DESC
LIMIT 10''')

print('Top 10 authors by number of articles:')
author_distribution.show()
