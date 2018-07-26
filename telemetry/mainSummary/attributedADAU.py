from pyspark.sql import SparkSession
from pyspark.sql.functions import *

### Problem to solve: We would like to understand the breakdown of DAU and ADAU by marketing acquisition method and how it has trended over time.

# Fields of interest: date, source, medium, campaign, content, DAU, ADAU

# Connecing to Main Sumamry
spark = SparkSession.builder.appName('mainSummary').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')

# Select Columns for marketing attributable DAU breakdown
acquisition = mainSummaryTable.filter((mainSummaryTable['submission_date_s3'] >= '20170101') & (mainSummaryTable['submission_date_s3'] <= '20170430')).select('submission_date_s3', 'client_id', 'attribution.source', 'attribution.medium', 'attribution.campaign', 'attribution.content', 'scalar_parent_browser_engagement_total_uri_count')

# Aggregate total pageviews by client ID
aggAcquisition = acquisition.groupBy('submission_date_s3', 'client_id', 'source', 'medium', 'campaign', 'content').agg(sum(acquisition.scalar_parent_browser_engagement_total_uri_count).alias('totalURI'))

# Calculate DAU, total page views, ADAU by day and marketing attribution channels
dailyDAU2017JanApr = aggAcquisition.groupBy('submission_date_s3', 'source', 'medium', 'campaign', 'content').agg(countDistinct(aggAcquisition.client_id).alias('DAU'), sum(aggAcquisition.totalURI).alias('totalURI'), sum(when(aggAcquisition['totalURI'] >= 5, 1).otherwise(0)).alias('activeDAU'))

# Write file to CSV
dailyDAU2017JanApr.write.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/dailyDAU2017JanApr.csv', header=False)
dailyDAU2017MayAug.write.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/dailyDAU2017MayAug.csv', header=False)
dailyDAU2017SepDec.write.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/dailyDAU2017SepDec.csv', header=False)