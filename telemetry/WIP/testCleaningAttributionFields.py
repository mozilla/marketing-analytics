from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType, StringType
from datetime import timedelta, datetime
import urllib

spark = SparkSession.builder.appName('mainSummary').getOrCreate()
mainSummaryTable = spark.read.option('mergeSchema', 'true').parquet('s3://telemetry-parquet/main_summary/v4/')
newProfilesTable = spark.read.option('mergeSchema', 'true').parquet('s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-new-profile-parquet/v2/')

columns = ['attribution.source',
           'attribution.medium',
           'attribution.campaign',
           'attribution.content',
           'submission_date_s3']

# Filter mainSummaryTable to selected columns and smaller date selection
testData = mainSummaryTable.select(columns)
testData = testData.filter('submission_date_s3 == 20180801')

#TODO: Current method of mediumCleanup and sourceCleanup takes too long to run. Can it be done using dataframes??
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
        return x['medium']


def sourceCleanup(x):
    # Cleanup direct traffic to equal (none)
    if x['source'] == 'www.mozilla.org' and x['medium'] == '%2528none%2529':
        return '(direct)'
     # Cleanup organic traffic medium to organic
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
        return x['source']