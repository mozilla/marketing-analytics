from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType, StringType

spark = SparkSession.builder.appName('mainSummary').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')

columns = ['submission_date_s3', 'client_id', 'scalar_parent_browser_engagement_total_uri_count']
df = mainSummaryTable.select(columns)

df = df.filter("submission_date_s3 >= '20180601' AND submission_date_s3 <= '20180624'")

uri = df.groupBy('submission_date_s3', 'client_id').agg(sum(
    df.scalar_parent_browser_engagement_total_uri_count).alias('totalURI'))


uriDaysActive = uri.groupBy('client_id').agg(countDistinct(uri.submission_date_s3).alias('daysWithActivity'), sum(uri.totalURI).alias('totalURI'))
uriDaysActive = uriDaysActive.withColumn('avgDailyURI', col=(uriDaysActive.totalURI / uriDaysActive.daysWithActivity))
