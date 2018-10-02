from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField
from datetime import timedelta, datetime
import pandas as pd
import numpy as np
import urllib

#TODO: Calculate Install Date for use in determining New User aDAU
#TODO: Add Dimensionality to metrics and maintain integrity (split this up to multiple tasks once ready to start)
#TODO: When adding country and attribution dimensions, getting a few additional DAU, ADAU etc. Figure out how to maintain totals as dimensions increase

# Connect to Main Summary & New Profiles Tables
spark = SparkSession.builder.appName('mainSummary').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')
newProfilesTable = spark.read.option('mergeSchema', 'true').parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-new-profile-parquet/v2/')

# Reduce Main Summary to columns of interest and store in new dataframe
mkgDimensionColumns = ['submission_date_s3',
                       'previous_subsession_id',
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
aggPageviews = mkgMainSummary.groupBy('submission_date_s3', 'client_id', 'source', 'medium', 'campaign', 'content', 'country').agg(sum(mkgMainSummary.scalar_parent_browser_engagement_total_uri_count).alias('totalURI'),
                                                                             sum(mkgMainSummary.numberSearches).alias('searches'))

#TODO: When ready to operationalize and don't need all historic data -> filter mainSummaryTable to speed up processing

# Retrieve Data in Chunks
#TODO: Create a function for the calculate metrics section instead of duplicating code
startPeriod = datetime(year=2018, month=7, day=1)
endPeriod = datetime(year=2018, month=7, day=2)
period = endPeriod - startPeriod

if period.days <= 30:
    startPeriodString = startPeriod.strftime("%Y%m%d")
    endPeriodString = endPeriod.strftime("%Y%m%d")

    # Calculate Metrics
    metrics = aggPageviews.groupBy('submission_date_s3', 'source', 'medium', 'campaign', 'content', 'country').agg(
        countDistinct(aggPageviews.client_id).alias('DAU'),
        sum(when(aggPageviews['totalURI'] >= 5, 1).otherwise(0)).alias('activeDAU'),
        sum(aggPageviews.totalURI).alias('totalURI'),
        sum(aggPageviews.searches).alias('searches'))
    metrics = metrics.filter(
        "submission_date_s3 >= '{}' AND submission_date_s3 <= '{}'".format(startPeriodString, endPeriodString)).select(
        'submission_date_s3', 'source', 'medium', 'campaign', 'content', 'country', 'DAU', 'activeDAU', 'totalURI',
        'searches')
    metrics = metrics.na.fill("unknown")  # Replace all blanks with unknown to ensure accurate joining

    # Determining New Profiles / Installs
    installs = newProfilesTable.groupBy('submission', 'environment.settings.attribution.source',
                                        'environment.settings.attribution.medium',
                                        'environment.settings.attribution.campaign',
                                        'environment.settings.attribution.content', 'metadata.geo_country').agg(
        countDistinct(newProfilesTable.client_id).alias('installs'))
    installs = installs.select(col('submission'), col('source').alias('npSource'), col('medium').alias('npMedium'),
                               col('campaign').alias('npCampaign'), col('content').alias('npContent'),
                               col('geo_country'), col('installs'))
    installs = installs.filter(
        "submission >= '{}' AND submission <= '{}'".format(startPeriodString, endPeriodString)).select('submission',
                                                                                                       'npSource',
                                                                                                       'npMedium',
                                                                                                       'npCampaign',
                                                                                                       'npContent',
                                                                                                       'geo_country',
                                                                                                       'installs')
    installs = installs.na.fill("unknown")  # Replace all blanks with unknown to ensure accurate joining

    # Join installs to metrics
    metrics = metrics.alias('metrics')
    installs = installs.alias('installs')
    metricsJoin = metrics.join(installs, (metrics.submission_date_s3 == installs.submission) & (
        metrics.source == installs.npSource) &
                               (metrics.medium == installs.npMedium) & (metrics.campaign == installs.npCampaign) &
                               (metrics.content == installs.npContent) & (metrics.country == installs.geo_country),
                               'outer')
    metricsJoin = metricsJoin.drop('submission', 'npSource', 'npMedium', 'npCampaign', 'npContent', 'geo_country')

    #TODO: Write a function for transforming the data
    # Clean Up Dimensions to Enable Joining with Google Analytics
    # Convert to pandas dataframe
    metricsDF = metricsJoin.toPandas()

    # Remove rows without dimensions'
    metricsDF = metricsDF.loc[metricsDF['submission_date_s3'].notnull()]

    # Change Nulls to 0
    metricsDF['DAU'].fillna(0, inplace=True)
    metricsDF['activeDAU'].fillna(0, inplace=True)
    metricsDF['totalURI'].fillna(0, inplace=True)
    metricsDF['searches'].fillna(0, inplace=True)
    metricsDF['installs'].fillna(0, inplace=True)

    # Clean up attribution fields
    # Remove URL Encoding from attribution fields - duplicate columns to preserve original telemetry columns

    metricsDF['sourceCleaned'] = list(
        map(lambda x: urllib.unquote_plus(urllib.unquote_plus(x).decode('utf8')), metricsDF['source']))
    metricsDF['mediumCleaned'] = list(
        map(lambda x: urllib.unquote_plus(urllib.unquote_plus(x).decode('utf8')), metricsDF['medium']))
    metricsDF['campaignCleaned'] = list(
        map(lambda x: urllib.unquote_plus(urllib.unquote_plus(x).decode('utf8')), metricsDF['campaign']))
    metricsDF['contentCleaned'] = list(
        map(lambda x: urllib.unquote_plus(urllib.unquote_plus(x).decode('utf8')), metricsDF['content']))

    # Convert floats to integer and clean up schema
    metricsDF['submission_date_s3'] = metricsDF.submission_date_s3.astype(int)
    metricsDF['submission_date_s3'] = metricsDF.submission_date_s3.astype(str)
    metricsDF['source'] = metricsDF.source.astype(str)
    metricsDF['medium'] = metricsDF.medium.astype(str)
    metricsDF['campaign'] = metricsDF.campaign.astype(str)
    metricsDF['content'] = metricsDF.content.astype(str)
    metricsDF['country'] = metricsDF.country.astype(str)
    metricsDF['DAU'] = metricsDF.DAU.astype(int)
    metricsDF['activeDAU'] = metricsDF.activeDAU.astype(int)
    metricsDF['totalURI'] = metricsDF.totalURI.astype(int)
    metricsDF['searches'] = metricsDF.searches.astype(int)
    metricsDF['installs'] = metricsDF.installs.astype(int)

    # Add column identifying moz vs dark funnel
    metricsDF['funnelOrigin'] = np.where((metricsDF['source'] == 'unknown') & (metricsDF['medium'] == 'unknown')
                                          & (metricsDF['campaign'] == 'unknown') & (
                                          metricsDF['content'] == 'unknown'),
                                          'darkFunnel', 'mozFunnel')

    def mediumCleanup(x):
        # Cleanup direct traffic to equal (none)
        if x['source'] == 'www.mozilla.org' and x['medium'] == '%2528none%2529':
            return '(none)'
            # Cleanup organic traffic medium to organic
        elif 'www.google' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
            return 'organic'
        elif 'bing.com' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
            return 'organic'
        elif 'yandex' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
            return 'organic'
        elif 'search.yahoo' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
            return 'organic'
        elif 'search.seznam' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
            return 'organic'
        else:
            return x['mediumCleaned']

    def sourceCleanup(x):
        # Cleanup direct traffic to equal (none)
        if x['source'] == 'www.mozilla.org' and x['medium'] == '%2528none%2529':
            return '(direct)'
        # Cleanup organic traffic sources
        elif 'www.google' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
            return 'google'
        elif 'bing.com' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
            return 'bing'
        elif 'yandex' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
            return 'yandex'
        elif 'search.yahoo' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
            return 'yahoo'
        elif 'search.seznam' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
            return 'seznam'
        else:
            return x['sourceCleaned']

    print('cleaning mediums')
    metricsDF['mediumCleaned'] = metricsDF.apply(mediumCleanup, axis=1)
    print('cleaning sources')
    metricsDF['sourceCleaned'] = metricsDF.apply(sourceCleanup, axis=1)

    # Join cleaned country list to df as countryName
    #TODO: Figure out how to read directly from s3 using pandas
    countryList = spark.read.csv('s3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/countryList/countryListStandardized.csv', sep=',', header=True)
    countryListDF = countryList.toPandas()

    # Make columns lower case to enable join
    countryListDF['rawCountry'] = map(unicode.lower, countryListDF.rawCountry)
    metricsDF['country'] = map(str.lower, metricsDF.country)

    metricsDF = pd.merge(metricsDF, countryListDF, left_on='country', right_on='rawCountry', how='left')

    # Drop unnecessary column and rename standardized country column
    metricsDF = metricsDF.drop('rawCountry', axis=1)
    metricsDF = metricsDF.rename(index=str, columns={'standardizedCountry': 'countryName'})
    metricsDF['countryName'] = metricsDF.countryName.astype(str)

    # Write File
    #TODO: Figure out how to directly write to s3 using pandas
    # Convert pandas df to spark df
    sparkMetricsDF = spark.createDataFrame(metricsDF)

    #write file

    sparkMetricsDF.coalesce(1).write.option("header", "true").csv(
        's3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyReporting/Dimensioned/{}-{}.csv'.format(
            startPeriodString, endPeriodString))
    print("{}-{} completed and saved".format(startPeriodString, endPeriodString))

else:
    dayChunks = timedelta(days=30)
    currentPeriodStart = startPeriod
    currentPeriodEnd = startPeriod + dayChunks
    currentPeriodChunk = endPeriod - currentPeriodEnd

    while currentPeriodEnd <= endPeriod:
        startPeriodString = currentPeriodStart.strftime("%Y%m%d")
        endPeriodString = currentPeriodEnd.strftime("%Y%m%d")

        # Calculate Metrics
        metrics = aggPageviews.groupBy('submission_date_s3', 'source', 'medium', 'campaign', 'content', 'country').agg(
            countDistinct(aggPageviews.client_id).alias('DAU'),
            sum(when(aggPageviews['totalURI'] >= 5, 1).otherwise(0)).alias('activeDAU'),
            sum(aggPageviews.totalURI).alias('totalURI'),
            sum(aggPageviews.searches).alias('searches'))
        metrics = metrics.filter(
            "submission_date_s3 >= '{}' AND submission_date_s3 <= '{}'".format(startPeriodString,
                                                                               endPeriodString)).select(
            'submission_date_s3', 'source', 'medium', 'campaign', 'content', 'country', 'DAU', 'activeDAU', 'totalURI',
            'searches')
        metrics = metrics.na.fill("unknown")  # Replace all blanks with unknown to ensure accurate joining

        # Determining New Profiles / Installs
        installs = newProfilesTable.groupBy('submission', 'environment.settings.attribution.source',
                                            'environment.settings.attribution.medium',
                                            'environment.settings.attribution.campaign',
                                            'environment.settings.attribution.content', 'metadata.geo_country').agg(
            countDistinct(newProfilesTable.client_id).alias('installs'))
        installs = installs.select(col('submission'), col('source').alias('npSource'), col('medium').alias('npMedium'),
                                   col('campaign').alias('npCampaign'), col('content').alias('npContent'),
                                   col('geo_country'), col('installs'))
        installs = installs.filter(
            "submission >= '{}' AND submission <= '{}'".format(startPeriodString, endPeriodString)).select('submission',
                                                                                                           'npSource',
                                                                                                           'npMedium',
                                                                                                           'npCampaign',
                                                                                                           'npContent',
                                                                                                           'geo_country',
                                                                                                           'installs')
        installs = installs.na.fill("unknown")  # Replace all blanks with unknown to ensure accurate joining

        # Join installs to metrics
        metrics = metrics.alias('metrics')
        installs = installs.alias('installs')
        metricsJoin = metrics.join(installs, (metrics.submission_date_s3 == installs.submission) & (
            metrics.source == installs.npSource) &
                                   (metrics.medium == installs.npMedium) & (metrics.campaign == installs.npCampaign) &
                                   (metrics.content == installs.npContent) & (metrics.country == installs.geo_country),
                                   'outer')
        metricsJoin = metricsJoin.drop('submission', 'npSource', 'npMedium', 'npCampaign', 'npContent', 'geo_country')

        # TODO: Write a function for transforming the data
        # Clean Up Dimensions to Enable Joining with Google Analytics
        # Convert to pandas dataframe
        metricsDF = metricsJoin.toPandas()

        # Remove rows without dimensions'
        metricsDF = metricsDF.loc[metricsDF['submission_date_s3'].notnull()]

        # Change Nulls to 0
        metricsDF['DAU'].fillna(0, inplace=True)
        metricsDF['activeDAU'].fillna(0, inplace=True)
        metricsDF['totalURI'].fillna(0, inplace=True)
        metricsDF['searches'].fillna(0, inplace=True)
        metricsDF['installs'].fillna(0, inplace=True)

        # Clean up attribution fields
        # Remove URL Encoding from attribution fields - duplicate columns to preserve original telemetry columns

        metricsDF['sourceCleaned'] = list(
            map(lambda x: urllib.unquote_plus(urllib.unquote_plus(x).decode('utf8')), metricsDF['source']))
        metricsDF['mediumCleaned'] = list(
            map(lambda x: urllib.unquote_plus(urllib.unquote_plus(x).decode('utf8')), metricsDF['medium']))
        metricsDF['campaignCleaned'] = list(
            map(lambda x: urllib.unquote_plus(urllib.unquote_plus(x).decode('utf8')), metricsDF['campaign']))
        metricsDF['contentCleaned'] = list(
            map(lambda x: urllib.unquote_plus(urllib.unquote_plus(x).decode('utf8')), metricsDF['content']))

        # Convert floats to integer and clean up schema
        metricsDF['submission_date_s3'] = metricsDF.submission_date_s3.astype(int)
        metricsDF['submission_date_s3'] = metricsDF.submission_date_s3.astype(str)
        metricsDF['source'] = metricsDF.source.astype(str)
        metricsDF['medium'] = metricsDF.medium.astype(str)
        metricsDF['campaign'] = metricsDF.campaign.astype(str)
        metricsDF['content'] = metricsDF.content.astype(str)
        metricsDF['country'] = metricsDF.country.astype(str)
        metricsDF['DAU'] = metricsDF.DAU.astype(int)
        metricsDF['activeDAU'] = metricsDF.activeDAU.astype(int)
        metricsDF['totalURI'] = metricsDF.totalURI.astype(int)
        metricsDF['searches'] = metricsDF.searches.astype(int)
        metricsDF['installs'] = metricsDF.installs.astype(int)

        # Add column identifying moz vs dark funnel
        metricsDF['funnelOrigin'] = np.where((metricsDF['source'] == 'unknown') & (metricsDF['medium'] == 'unknown')
                                             & (metricsDF['campaign'] == 'unknown') & (
                                                 metricsDF['content'] == 'unknown'),
                                             'darkFunnel', 'mozFunnel')


        def mediumCleanup(x):
            # Cleanup direct traffic to equal (none)
            if x['source'] == 'www.mozilla.org' and x['medium'] == '%2528none%2529':
                return '(none)'
                # Cleanup organic traffic medium to organic
            elif 'www.google' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
                return 'organic'
            elif 'bing.com' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
                return 'organic'
            elif 'yandex' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
                return 'organic'
            elif 'search.yahoo' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
                return 'organic'
            elif 'search.seznam' in x['source'] and x['medium'] == 'referral' and x[
                'campaign'] == '%2528not%2Bset%2529':
                return 'organic'
            else:
                return x['mediumCleaned']


        def sourceCleanup(x):
            # Cleanup direct traffic to equal (none)
            if x['source'] == 'www.mozilla.org' and x['medium'] == '%2528none%2529':
                return '(direct)'
            # Cleanup organic traffic sources
            elif 'www.google' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
                return 'google'
            elif 'bing.com' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
                return 'bing'
            elif 'yandex' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
                return 'yandex'
            elif 'search.yahoo' in x['source'] and x['medium'] == 'referral' and x['campaign'] == '%2528not%2Bset%2529':
                return 'yahoo'
            elif 'search.seznam' in x['source'] and x['medium'] == 'referral' and x[
                'campaign'] == '%2528not%2Bset%2529':
                return 'seznam'
            else:
                return x['sourceCleaned']


        print('cleaning mediums')
        metricsDF['mediumCleaned'] = metricsDF.apply(mediumCleanup, axis=1)
        print('cleaning sources')
        metricsDF['sourceCleaned'] = metricsDF.apply(sourceCleanup, axis=1)

        # Join cleaned country list to df as countryName
        # TODO: Figure out how to read directly from s3 using pandas
        countryList = spark.read.csv(
            's3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/countryList/countryListStandardized.csv', sep=',',
            header=True)
        countryListDF = countryList.toPandas()

        # Make columns lower case to enable join
        countryListDF['rawCountry'] = map(unicode.lower, countryListDF.rawCountry)
        metricsDF['country'] = map(str.lower, metricsDF.country)

        metricsDF = pd.merge(metricsDF, countryListDF, left_on='country', right_on='rawCountry', how='left')

        # Drop unnecessary column and rename standardized country column
        metricsDF = metricsDF.drop('rawCountry', axis=1)
        metricsDF = metricsDF.rename(index=str, columns={'standardizedCountry': 'countryName'})
        metricsDF['countryName'] = metricsDF.countryName.astype(str)

        # Write File
        # TODO: Figure out how to directly write to s3 using pandas
        # Convert pandas df to spark df
        sparkMetricsDF = spark.createDataFrame(metricsDF)

        # write file

        sparkMetricsDF.coalesce(1).write.option("header", "true").csv(
            's3://net-mozaws-prod-us-west-2-pipeline-analysis/gkabbz/weeklyReporting/Dimensioned/{}-{}.csv'.format(
                startPeriodString, endPeriodString))
        print("{}-{} completed and saved".format(startPeriodString, endPeriodString))

        # Set new dates for next loop
        currentPeriodStart = currentPeriodEnd + timedelta(days=1)
        currentPeriodEnd = currentPeriodStart + dayChunks
        if currentPeriodStart > endPeriod:
            break
        else:
            if currentPeriodEnd > endPeriod:
                currentPeriodEnd = endPeriod


