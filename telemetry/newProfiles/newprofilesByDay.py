from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('newProfiles').getOrCreate()
newProfilesTable = spark.read.option('mergeSchema', 'true').parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-new-profile-parquet/v2/')
print("connected to table")
metrics = newProfilesTable.filter("submission >= '20180701' AND submission <= '20180715'")
metrics = metrics.groupBy('submission', 'metadata.geo_country', 'metadata.geo_city', 'environment.settings.attribution.source', 'environment.settings.attribution.medium', 'environment.settings.attribution.campaign', 'environment.settings.attribution.content').agg(countDistinct(metrics.client_id).alias('installsNewProfiles'))
print("writing to csv")
metrics.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/installsMay2018.csv')


metrics = metrics.groupBy('submission').agg(countDistinct(metrics.client_id).alias('installsNewProfiles'))