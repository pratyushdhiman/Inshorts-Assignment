from spark_utils import get_spark
spark = get_spark("ins_data_analysis")

user_silver = spark.read.parquet("/Users/pd_10/Desktop/gcs_data/silver/user")

user_silver.createOrReplaceTempView("user_silver")

# Total number of users

total_users=spark.sql(f'''SELECT COUNT(DISTINCT deviceid) AS total_users
FROM user_silver''')

print('Total number of users:')
total_users.show()

#user installs over time

####Daily
daily_install=spark.sql(f'''SELECT install_dt, COUNT(DISTINCT deviceid) AS installs
FROM user_silver
GROUP BY install_dt
ORDER BY install_dt''')

print('user installs daily:')
daily_install.show()

####Weekly

weekly_install=spark.sql('''SELECT YEAR(install_dt) AS year, WEEKOFYEAR(install_dt) AS week,
       COUNT(DISTINCT deviceid) AS installs
FROM user_silver
GROUP BY year, week
ORDER BY year, week''')

print('user installs weekly:')
weekly_install.show()

####Monthly

monthly_install=spark.sql('''SELECT YEAR(install_dt) AS year, MONTH(install_dt) AS month,
       COUNT(DISTINCT deviceid) AS installs
FROM user_silver
GROUP BY year, month
ORDER BY year, month''')

print('user installs monthly:')
monthly_install.show()

#user distribution by platform

platform_distribution=spark.sql(f'''SELECT platform, COUNT(DISTINCT deviceid) AS users
FROM user_silver
GROUP BY platform
ORDER BY users DESC''')

print('user distribution by platform:')
platform_distribution.show()

#Distribution of Users by top 10 Language and districts

lan_distribution=spark.sql(f'''SELECT lang, district, COUNT(DISTINCT deviceid) AS users
FROM user_silver
GROUP BY lang, district
ORDER BY users DESC
limit 10''')

print('Distribution of Users by top 10 Language and districts:')
lan_distribution.show()

#Distribution of Users by campaign_id including organic

campaign_type_distribution=spark.sql(f'''SELECT COALESCE(campaign_id, 'organic') AS campaign,
       COUNT(DISTINCT deviceid) AS users
FROM user_silver
GROUP BY COALESCE(campaign_id, 'organic')
ORDER BY users DESC''')

print('Distribution of Users by campaign_id including organic:')
campaign_type_distribution.show()