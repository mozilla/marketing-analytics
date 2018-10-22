from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType, StringType
from datetime import timedelta, datetime

# Connect to Main Summary
spark = SparkSession.builder.appName('mainSummary').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')

# Reduce Main Summary to columns of interest and store in new dataframe
mkgDimensionColumns = ['submission_date_s3',
                       'profile_creation_date',
                       'install_year',
                       'client_id',
                       'profile_subsession_counter',
                       'sample_id',
                       'channel',
                       'normalized_channel',
                       'country',
                       'geo_subdivision1',
                       'city',
                       'attribution.source',
                       'attribution.medium',
                       'attribution.campaign',
                       'attribution.content',
                       'os',
                       'os_version',
                       'app_name',
                       'windows_build_number',
                       'scalar_parent_browser_engagement_total_uri_count',
                       'search_counts'
                       ]

mkgMainSummary = mainSummaryTable.select(mkgDimensionColumns)
mkgMainSummary = mkgMainSummary.filter("submission_date_s3 == '20181015'")

# Calculate Searches
## Function for getting search counts (per jupyter notebook from Su-Young Hong)

searchSources = ['urlbar', 'searchbar', 'abouthome', 'newtab', 'contextmenu', 'system', 'activitystream']

def get_searches(array):
  searches = 0
  if array:
    for item in array:
      if item['source'] in searchSources:
        if item['count']:
          searches += item['count']
  return searches

returnSchema = IntegerType()
spark.udf.register('get_searches', get_searches, returnSchema)
get_searches_udf = udf(lambda search: get_searches(search), returnSchema)

## Calculate number of searches and append to mkgMainSummary data frame
mkgMainSummary = mkgMainSummary.withColumn('numberSearches', get_searches_udf(col('search_counts'))).drop(col('search_counts'))

# Aggregate total pageviews by client ID
aggPageviews = mkgMainSummary.groupBy('submission_date_s3', 'client_id').agg(sum(mkgMainSummary.scalar_parent_browser_engagement_total_uri_count).alias('totalURI'),
                                                                             sum(mkgMainSummary.numberSearches).alias('searches'))

# Calculate Metrics
metrics = aggPageviews.groupBy('submission_date_s3').agg(sum(when(aggPageviews['totalURI'] >= 5, 1).otherwise(
                                                                 0)).alias('activeDAU'),
                                                             sum(aggPageviews.totalURI).alias('totalURI'),
                                                             sum(aggPageviews.searches).alias('searches'))

# Replace all blanks with unknown to ensure accurate joining
metrics = metrics.na.fill("unknown")
metrics = metrics.re


# Open csv File from redash snippet data collection
snippets = spark.read.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/testSnippet/testSnippet20181015.csv', sep=',', header=True)
snippets = snippets.withColumn('clientIDCleaned', substring('client_id', 2, 36))
snippets = snippets.drop('client_id')

#join metrics to snippets
snippets = snippets.alias('snippets')
metrics = metrics.alias('metrics')

snippetsJoin = snippets.join(metrics, snippets.clientIDCleaned == metrics.client_id, 'left')

# Write snippetsJoin file to csv

snippetsJoin.coalesce(1).write.option("header", "true").csv(
        's3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/testSnippet/testJoin3.csv')

# Remove parentheses from client_id in snippets


metrics.coalesce(1).write.option("header", "true").csv(
        's3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyReporting/Dimensioned/testSnippetsClients20181015.csv')