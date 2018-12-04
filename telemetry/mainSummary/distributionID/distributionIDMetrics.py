from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType, StringType
from datetime import timedelta, datetime

# Connect to Main Summary & New Profiles Tables
spark = SparkSession.builder.appName('mainSummary').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')
newProfilesTable = spark.read.option('mergeSchema', 'true').parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-new-profile-parquet/v2/')

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
                       'distribution_id',
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
aggPageviews = mkgMainSummary.groupBy('submission_date_s3', 'client_id', 'distribution_id', 'country').agg(sum(mkgMainSummary.scalar_parent_browser_engagement_total_uri_count).alias('totalURI'),
                                                                             sum(mkgMainSummary.numberSearches).alias('searches'))

distroUsage = aggPageviews.filter("submission_date_s3 >= '20181126' AND submission_date_s3 <= '20181203' AND distribution_id != ''")


distroUsage = distroUsage.groupBy('submission_date_s3', 'distribution_id', 'country').agg(
        countDistinct(distroUsage.client_id).alias('DAU'),
        sum(when(distroUsage['totalURI'] >= 5, 1).otherwise(0)).alias('activeDAU'),
        sum(distroUsage.totalURI).alias('totalURI'),
        sum(distroUsage.searches).alias('searches'))

distroInstalls = newProfilesTable.groupBy('submission', 'environment.partner.distribution_id', 'metadata.geo_country').agg(countDistinct(newProfilesTable.client_id).alias('installs'))
distroInstalls = distroInstalls.select(col('submission'), col('distribution_id').alias('npDistributionID'), col('geo_country'), col('installs'))

distroUsage = distroUsage.alias('distroUsage')
distroInstalls = distroInstalls.alias('distroInstalls')

distroJoin = distroUsage.join(distroInstalls, (distroUsage.submission_date_s3 == distroInstalls.submission) &
                              (distroUsage.distribution_id == distroInstalls.npDistributionID) & (distroUsage.country == distroInstalls.geo_country), 'left')

distroJoin = distroJoin.drop('submission', 'npDistributionID', 'geo_country')

# Write File
distroJoin.coalesce(1).write.option("header", "true").csv(
        's3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/adHoc/distributionIDPerf20181125-20181203.csv')


#aws s3 sync s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/adHoc /home/hadoop/sparkAnalysis/adHoc
#rsync -av gkabbz-001:/home/hadoop/sparkAnalysis/adHoc /Users/gkaberere/spark-warehouse/adHoc
#gsutil cp /Users/gkaberere/spark-warehouse/adHoc/adHoc/distributionIDPerf20180930-20181122.csv/distributionIDPerf20180930-20181122.csv gs://gkabbz-upload


