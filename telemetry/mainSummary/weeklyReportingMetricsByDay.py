from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType, StringType
import datetime
import pandas as pd

#TODO: Calculate Installs
#TODO: Calculate Install Date for use in determining New User aDAU
#TODO: Add Dimensionality to metrics and maintain integrit (split this up to multiple tasks once ready to start)

# Connect to Main Summary & New Profiles Tables
spark = SparkSession.builder.appName('mainSummary').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')
newProfilesTable = spark.read.option('mergeSchema', 'true').parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-new-profile-parquet/v1/')

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
metrics = aggPageviews.groupBy('submission_date_s3').agg(countDistinct(aggPageviews.client_id).alias('DAU'),
                                                         sum(when(aggPageviews['totalURI'] >= 5, 1).otherwise(0)).alias('activeDAU'),
                                                         sum(aggPageviews.totalURI).alias('totalURI'),
                                                         sum(aggPageviews.searches).alias('searches'))



#TODO: When ready to operationalize and don't need all historic data -> filter mainSummaryTable to speed up processing
#TODO: Code to iterate through periods and write output to file to speed up pulling historic data

# Retrieve Data in Chunks
periodStart = '20180101'
periodEnd = '20180331'

metrics = metrics.filter("submission_date_s3 >= '20171101' AND submission_date_s3 <= '20171231'").select('submission_date_s3', 'DAU', 'activeDAU', 'totalURI', 'searches')
# Determining New Profiles / Installs
installs = newProfilesTable.groupBy('submission').agg(countDistinct(newProfilesTable.client_id).alias('installs'))
installs = installs.filter("submission >= '20171101' AND submission <= '20171231'").select('submission', 'installs')
# Join installs to metrics
metrics = metrics.alias('metrics')
installs = installs.alias('installs')
metricsJoin = metrics.join(installs, col('metrics.submission_date_s3') == col('installs.submission'), 'outer')
metricsJoin = metricsJoin.drop('submission')


# Determining Install Date
## Aggregate new profiles table by submission date and client id
clientAcquisition = newProfilesTable.select('submission', 'client_id').dropDuplicates()
# Rename columns to avoid naming conflicts post join with Main Summary
clientAcquisition = clientAcquisition.withColumn('newProfSubmissionDate', col('submission')).drop('submission')
clientAcquisition = clientAcquisition.withColumn('newProfileClientid', col('client_id')).drop('client_id')

# TODO: Check if clientAcquisition has clients that show up multiple times. I.e. same client_id different dates
## Join clientAcquisition to mkgMainSummary
mkgMainSummary = mkgMainSummary.alias('mkgMainSummary')
clientAcquisition = clientAcquisition.alias('clientAcquisition')
mkgMainSummaryJoin = mkgMainSummary.join(clientAcquisition, col('mkgMainSummary.client_id') == col('clientAcquisition.newProfileClientid'), 'outer')
mkgMainSummaryJoin.persist()

# Write file
metricsJoin.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyReportingJoinDataNovDec2017.csv')
metricsJoin.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyReportingJoinDataAprJun2018v2.csv')
metricsJoin.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyReportingJoinData1H2017.csv')
metricsJoin.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyReportingJoinDataAprJun2018.csv')
metricsJoin.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyReportingJoinDatav2.csv')
metrics.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyReportingJoinData.csv')
metrics.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyReportingJan2018.csv')
metrics.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyReportingJanMay2018.csv')
metrics.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/weeklyReportingApr-May2018v3.csv')
metrics.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/weeklyReportingQ12017v2.csv')
metrics.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/weeklyReportingQ22017v2.csv')
metrics.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/weeklyReportingQ32017v2.csv')
metrics.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/weeklyReportingQ42017v2.csv')

# Graveyard
# ToDO: Delete when production ready

testInstallCalc = mkgMainSummary.filter(mkgMainSummary['submission_date_s3'] == '20180415').select('submission_date_s3', 'profile_creation_date', 'install_year', 'client_id')
testInstallCalc = testInstallCalc.withColumn('unixInstallDate', col('profile_creation_date')*86400)
testInstallCalc = testInstallCalc.withColumn('installDate', from_unixtime('unixInstallDate').cast(DateType()))
testInstallCalc = testInstallCalc.withColumn('installDateString', from_unixtime('unixInstallDate', format='yyyyMMdd').cast(StringType()))
testInstallCalc = testInstallCalc.withColumn('calculatedInstallDate', year('installDate'))
testInstallCalc = testInstallCalc.withColumn('installYearVariance', col('calculatedInstallDate')-col('install_year'))

testInstallMetric = mkgMainSummary.filter(mkgMainSummary['submission_date_s3'] == '20180415').select('submission_date_s3', 'installDate', 'client_id')
# group by client ID
testInstallMetric2 = testInstallMetric.filter(testInstallMetric['submission_date_s3'] == testInstallMetric['installDate'])

# Test using profile_subsession_counter to determine intall date

# for April 15 new_profiles table has 631,553 new profiles

testPSC = mkgMainSummary.filter(mkgMainSummary['submission_date_s3'] == '20180415').select('submission_date_s3', 'profile_subsession_counter', 'client_id')
testInstalls = testPSC.groupBy('submission_date_s3').agg(sum(when(testPSC['profile_subsession_counter'] == 1, 1).otherwise(0)).alias('installs'))
#testInstalls results in a number that's about double - 1,114,655

testPSC2 = testPSC.dropDuplicates()
testInstalls2 = testPSC2.groupBy('submission_date_s3').agg(sum(when(testPSC2['profile_subsession_counter'] == 1, 1).otherwise(0)).alias('installs'))
# Test installs 2 reduces the number to 977,620 but still high

testPSC3 = testPSC.withColumn('instalFlag', when(testPSC['profile_subsession_counter'] == 1, 1).otherwise(0))
testInstalls3 = testPSC3.filter(testPSC3['instalFlag'] == 1).groupBy('submission_date_s3').agg(countDistinct(testPSC3.client_id))
# Test installs 3 stays same as 2 at 977,620

## What to do? What to do? Might be interesting to understand the data in mainSummary. What causes the added rows by day?
testData = mainSummaryTable.filter(mainSummaryTable['submission_date_s3'] == '20180415')
testData = testData.orderBy('client_id').collect(50000)

testData.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/testData041518.csv')

def flatten_df(nested_df, layers):
    flat_cols = []
    nested_cols = []
    flat_df = []

    flat_cols.append([c[0] for c in nested_df.dtypes if c[1][:6] != 'struct'])
    nested_cols.append([c[0] for c in nested_df.dtypes if c[1][:6] == 'struct'])

    flat_df.append(nested_df.select(flat_cols[0] +
                               [col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols[0]
                                for c in nested_df.select(nc+'.*').columns])
                  )
    for i in range(1, layers):
        print (flat_cols[i-1])
        flat_cols.append([c[0] for c in flat_df[i-1].dtypes if c[1][:6] != 'struct'])
        nested_cols.append([c[0] for c in flat_df[i-1].dtypes if c[1][:6] == 'struct'])

        flat_df.append(flat_df[i-1].select(flat_cols[i] +
                                [col(nc+'.'+c).alias(nc+'_'+c)
                                    for nc in nested_cols[i]
                                    for c in flat_df[i-1].select(nc+'.*').columns])
        )

    return flat_df[-1]

flattenedTestData = flatten_df(testData, 6)

mkgDimensionColumns = ['submission_date_s3',
                       'profile_creation_date',
                       'install_year',
                       'client_id',
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
                       'app_version',
                       'windows_build_number',
                       'scalar_parent_browser_engagement_total_uri_count',
                       'session_id',
                       'subsession_id',
                       'previous_session_id',
                       'previous_subsession_id',
                       'subsession_counter',
                       'profile_subsession_counter',
                       'session_start_date',
                       'subsession_start_date',
                       'session_length',
                       'subsession_length'
                       ]

mkgMainSummary = mainSummaryTable.select(mkgDimensionColumns)
mkgMainSummary = mkgMainSummary.orderBy('client_id').filter(mkgMainSummary['submission_date_s3'] == '20180415').limit(500000)
mkgMainSummary.coalesce(1).write.option("header", "true").csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/testData041518.csv')