from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Connecing to Main Sumamry
spark = SparkSession.builder.appName('mainSummary').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')

#1 Calculate Non Segmented DAU and ActiveDAU
    #1a. Calculate total URI by day by client ID
dailyDAU = mainSummaryTable.groupBy('submission_date_s3', 'client_id').agg({'scalar_parent_browser_engagement_total_uri_count':'sum'})
    #1b. Calculate DAU and activeDAU



# Selecting columns of interest for Marketing from MainSummary
mkgMainSummary = mainSummaryTable.select('submission_date_s3', 'profile_creation_date', 'install_year', 'client_id', 'sample_id', 'channel', 'normalized_channel', 'country', 'geo_subdivision1', 'city', 'locale', 'attribution.source', 'attribution.medium', 'attribution.campaign', 'attribution.content', 'os', 'os_version', 'windows_build_number', 'apple_model_id', 'active_addons_count', 'is_default_browser', 'scalar_parent_browser_engagement_total_uri_count')

# Grouping data so there's one row per dimension
mkgGroupedData = mkgMainSummary.groupBy('submission_date_s3', 'profile_creation_date', 'install_year', 'client_id', 'sample_id', 'channel', 'normalized_channel', 'country', 'geo_subdivision1', 'city', 'locale', 'source', 'medium', 'campaign', 'content', 'os', 'os_version', 'windows_build_number', 'apple_model_id', 'active_addons_count', 'is_default_browser').agg({'scalar_parent_browser_engagement_total_uri_count':'sum'})




#### TEMP in case kernel dies

dau2018.write.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/jan1activity.csv',header=False)
dau2018 = mainSummaryTable.filter((mainSummaryTable['submission_date_s3'] >= '20180101')).select('submission_date_s3', 'client_id')
dau2018 = mainSummaryTable.filter((mainSummaryTable['submission_date_s3'] == '20180101')).select('submission_date_s3', 'client_id', 'scalar_
       : parent_browser_engagement_total_uri_count')
dau2018Summary = dau2018.groupBy('submission_date_s3').agg(countDistinct(dau2018.client_id))


sample = mainSummaryTable.select('submission_date_s3', 'client_id', 'sample_id')