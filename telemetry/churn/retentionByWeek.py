from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType, StringType
from datetime import timedelta, datetime

spark = SparkSession.builder.appName('churn').getOrCreate()
churnTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/churn/v3')

# Select desired columns
weeklyRetentionColumns = ['acquisition_period', 'current_week', 'geo', 'channel', 'source', 'medium', 'campaign', 'content', 'n_profiles']
weeklyRetentionTable = churnTable.select(weeklyRetentionColumns)

# Filter weekly retention table for release installs only
weeklyRetentionTable = weeklyRetentionTable.filter("channel LIKE 'release%'")
weeklyRetentionTable = weeklyRetentionTable.filter("acquisition_period LIKE '2017-04%'")



# Group Results
weeklyRetention = weeklyRetentionTable.groupBy('acquisition_period', 'current_week', 'geo', 'channel', 'source', 'medium',
                                               'campaign', 'content').agg(sum(weeklyRetentionTable.n_profiles).alias('n_profiles'))

# Write File
weeklyRetention.coalesce(1).write.option("header", "false").csv(
        's3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/retention/JanApr/2017/201704.csv')


